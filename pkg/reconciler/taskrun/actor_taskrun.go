package taskrun

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"github.com/tektoncd/pipeline/pkg/internal/computeresources/tasklevel"
	podconvert "github.com/tektoncd/pipeline/pkg/pod"
	"github.com/tektoncd/pipeline/pkg/reconciler/taskrun/resources"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
)

const (
	actorStepPollRequeue = 500 * time.Millisecond
	actorInlineStepWait  = 1500 * time.Millisecond
)

func hasActorWorkspace(wbs []v1.WorkspaceBinding) bool {
	_, ok := findActorWorkspace(wbs)
	return ok
}

func findActorWorkspace(wbs []v1.WorkspaceBinding) (*v1.WorkspaceBinding, bool) {
	for i := range wbs {
		if wbs[i].Actor != nil {
			return &wbs[i], true
		}
	}
	return nil, false
}

func (c *Reconciler) getActorRuntime(ctx context.Context) (ateactor.Runtime, error) {
	if rt := ateactor.FromContext(ctx); rt != nil {
		return rt, nil
	}
	if c.ActorRuntime != nil {
		return c.ActorRuntime, nil
	}
	c.actorRuntimeMu.Lock()
	defer c.actorRuntimeMu.Unlock()
	if c.actorRuntime != nil {
		return c.actorRuntime, nil
	}
	rt, err := ateactor.NewClusterRuntime(ctx, c.KubeClientSet, ateactor.ClusterConfigFromEnv())
	if err != nil {
		return nil, err
	}
	c.actorRuntime = rt
	return rt, nil
}

func (c *Reconciler) failActorTaskRun(ctx context.Context, tr *v1.TaskRun, reason v1.TaskRunReason) {
	logger := logging.FromContext(ctx)
	terminateStepsInActor(tr, reason)

	actorWB, ok := findActorWorkspace(tr.Spec.Workspaces)
	if !ok {
		return
	}
	atespace := strings.TrimSpace(actorWB.Actor.Atespace)
	actorName := tr.Annotations[ateactor.AnnotationActorName]
	if actorName == "" {
		return
	}
	rt, err := c.getActorRuntime(ctx)
	if err != nil {
		logger.Warnf("Unable to obtain Substrate runtime while failing TaskRun %s/%s: %v", tr.Namespace, tr.Name, err)
		return
	}

	mode := tr.Annotations[ateactor.AnnotationActorMode]
	retain := tr.Annotations[ateactor.AnnotationRetainWorkspaceActor] == "true"
	if mode == ateactor.ActorModeTrunk || retain {
		if suspErr := ateactor.SuspendActorSync(ctx, rt.Control(), atespace, actorName); suspErr != nil {
			logger.Warnf("Failed to suspend Actor %s/%s on TaskRun %s/%s %s: %v", atespace, actorName, tr.Namespace, tr.Name, reason, suspErr)
		}
		return
	}
	if delErr := ateactor.DeleteActorIgnoreNotFound(ctx, rt.Control(), atespace, actorName); delErr != nil {
		logger.Warnf("Failed to delete standalone Actor %s/%s on TaskRun %s/%s %s: %v", atespace, actorName, tr.Namespace, tr.Name, reason, delErr)
	}
}

func terminateStepsInActor(tr *v1.TaskRun, reason v1.TaskRunReason) {
	finTime := metav1.Time{Time: time.Now()}
	if tr.Status.CompletionTime != nil {
		finTime = *tr.Status.CompletionTime
	}
	for i, step := range tr.Status.Steps {
		if step.Running != nil {
			step.Terminated = &corev1.ContainerStateTerminated{
				ExitCode:   1,
				StartedAt:  step.Running.StartedAt,
				FinishedAt: finTime,
				Reason:     reason.String(),
				Message:    fmt.Sprintf("Step %s terminated (%s)", step.Name, reason),
			}
			step.TerminationReason = reason.String()
			step.Running = nil
			tr.Status.Steps[i] = step
		} else if step.Waiting != nil {
			started := tr.CreationTimestamp
			if started.IsZero() {
				started = finTime
			}
			step.Terminated = &corev1.ContainerStateTerminated{
				ExitCode:   1,
				StartedAt:  started,
				FinishedAt: finTime,
				Reason:     reason.String(),
				Message:    fmt.Sprintf("Step %s terminated before start (%s)", step.Name, reason),
			}
			step.TerminationReason = reason.String()
			step.Waiting = nil
			tr.Status.Steps[i] = step
		}
	}
}

func (c *Reconciler) reconcileActorTaskRun(
	ctx context.Context,
	ts *v1.TaskSpec,
	tr *v1.TaskRun,
	rtr *resources.ResolvedTask,
) error {
	ctx, span := c.tracerProvider.Tracer(TracerName).Start(ctx, "reconcileActorTaskRun")
	defer span.End()
	logger := logging.FromContext(ctx)

	actorWB, ok := findActorWorkspace(tr.Spec.Workspaces)
	if !ok {
		return nil
	}
	actorSrc := actorWB.Actor
	atespace := strings.TrimSpace(actorSrc.Atespace)
	if atespace == "" {
		err := errors.New("actor workspace binding is missing atespace")
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
		return controller.NewPermanentError(err)
	}

	ts = ts.DeepCopy()
	if validateErr := v1.ValidateUsageOfDeclaredParameters(ctx, ts.Steps, ts.Params); validateErr != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, validateErr)
		return controller.NewPermanentError(validateErr)
	}
	if validateErr := ts.Validate(ctx); validateErr != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, validateErr)
		return controller.NewPermanentError(validateErr)
	}
	ts = resources.ApplyCredentialsPath(ts, pipeline.CredsDir)

	mergedSteps, err := v1.MergeStepsWithStepTemplate(ts.StepTemplate, ts.Steps)
	if err != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
		return controller.NewPermanentError(err)
	}
	mergedSteps, err = v1.MergeStepsWithSpecs(mergedSteps, tr.Spec.StepSpecs)
	if err != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
		return controller.NewPermanentError(err)
	}
	if tr.Spec.ComputeResources != nil {
		tasklevel.ApplyTaskLevelComputeResources(mergedSteps, tr.Spec.ComputeResources)
	}
	mergedSidecars, err := v1.MergeSidecarsWithSpecs(ts.Sidecars, tr.Spec.SidecarSpecs)
	if err != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
		return controller.NewPermanentError(err)
	}

	rt, err := c.getActorRuntime(ctx)
	if err != nil {
		return fmt.Errorf("initialize Substrate runtime: %w", err)
	}

	stepInputs := make([]ateactor.StepInput, len(mergedSteps))
	for i, s := range mergedSteps {
		cpuMilli, memBytes := extractResourceLimits(s.ComputeResources)
		stepInputs[i] = ateactor.StepInput{
			Name:          stepDisplayName(s.Name, i),
			Image:         s.Image,
			CpuMillicores: cpuMilli,
			MemoryBytes:   memBytes,
		}
	}
	sidecarInputs := make([]ateactor.SidecarInput, len(mergedSidecars))
	for i, sc := range mergedSidecars {
		cpuMilli, memBytes := extractResourceLimits(sc.ComputeResources)
		scEnv, envErr := c.resolveEnvVars(ctx, tr, sc.Env)
		if envErr != nil {
			return envErr
		}
		sidecarInputs[i] = ateactor.SidecarInput{
			Name:          sc.Name,
			Image:         sc.Image,
			Command:       sc.Command,
			Args:          sc.Args,
			Script:        sc.Script,
			Env:           scEnv,
			CpuMillicores: cpuMilli,
			MemoryBytes:   memBytes,
		}
	}

	synth, err := ateactor.SynthesizeTemplate(ctx, rt, ateactor.TemplateParams{
		Atespace:        atespace,
		SupervisorImage: rt.SupervisorImage(),
		WorkerSelector:  actorSrc.WorkerSelector,
		Steps:           stepInputs,
		Sidecars:        sidecarInputs,
	})
	if err != nil {
		tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
		return controller.NewPermanentError(err)
	}

	storedTmpl, err := ateactor.EnsureActorTemplate(ctx, rt.Control(), synth.Template)
	if err != nil {
		return err
	}

	if tr.Annotations == nil {
		tr.Annotations = make(map[string]string)
	}
	actorMode := tr.Annotations[ateactor.AnnotationActorMode]
	actorName := tr.Annotations[ateactor.AnnotationActorName]
	if actorName == "" {
		if actorMode == ateactor.ActorModeBranch {
			actorName = ateactor.SanitizeActorName("br", tr.Name)
		} else {
			actorMode = ateactor.ActorModeStandalone
			actorName = ateactor.StandaloneActorName(tr.Name)
		}
	} else if actorMode == "" {
		actorMode = ateactor.ActorModeTrunk
	}
	sourceTag := tr.Annotations[ateactor.AnnotationSourceTag]
	if sourceTag == "" {
		sourceTag = strings.TrimSpace(actorSrc.SourceTag)
	}

	actor, err := ateactor.EnsureActorReady(ctx, rt.Control(), ateactor.EnsureActorParams{
		Atespace:       atespace,
		ActorName:      actorName,
		Template:       storedTmpl,
		SourceTag:      sourceTag,
		WorkerSelector: actorSrc.WorkerSelector,
		Namespace:      tr.Namespace,
		EgressSecret:   actorSrc.EgressSecret,
		EgressHosts:    actorSrc.EgressHosts,
	})
	if err != nil {
		return err
	}

	tr.Annotations[ateactor.AnnotationAtespace] = atespace
	tr.Annotations[ateactor.AnnotationActorMode] = actorMode
	tr.Annotations[ateactor.AnnotationActorName] = actorName
	tr.Annotations[ateactor.AnnotationActorTemplate] = storedTmpl.GetMetadata().GetName()
	if wp := actor.GetStatus().GetWorkerAssignment().GetWorkerPod(); wp != "" {
		tr.Annotations[ateactor.AnnotationWorkerPod] = wp
	}

	supClient := rt.SupervisorClient(atespace, actorName)
	if tr.Annotations[ateactor.AnnotationSeededWorkspaces] != "true" {
		if err := c.seedActorWorkspaces(ctx, tr, ts, supClient); err != nil {
			return err
		}
		tr.Annotations[ateactor.AnnotationSeededWorkspaces] = "true"
	}

	now := metav1.Time{Time: c.Clock.Now()}
	if tr.Status.StartTime == nil {
		tr.Status.StartTime = &now
	}
	ensureStepAndSidecarStatuses(tr, mergedSteps, mergedSidecars, synth, now)
	tr.Status.MarkResourceOngoing(v1.TaskRunReasonRunning, "Not all Steps in the Task have finished executing")

	resultNames := collectDeclaredResultNames(ts)
	defaultWorkDir := resolveActorWorkspaceMountPath(ts, actorWB)

	for i, step := range mergedSteps {
		if tr.Status.Steps[i].Terminated != nil {
			continue
		}

		stepID := actorStepID(tr, i)
		tr.Annotations[ateactor.AnnotationCurrentStepID] = stepID

		st, getErr := supClient.GetStep(ctx, stepID)
		if getErr != nil {
			if !ateactor.IsStepNotFound(getErr) {
				return fmt.Errorf("query step %q on Actor %s/%s: %w", stepID, atespace, actorName, getErr)
			}
			stepEnv, envErr := c.resolveEnvVars(ctx, tr, step.Env)
			if envErr != nil {
				return envErr
			}
			var targetAddr string
			if containerIdx := synth.StepContainerIndices[i]; containerIdx > 0 {
				targetAddr = ateactor.WorkerLoopbackAddr(containerIdx)
			}
			workDir := step.WorkingDir
			if strings.TrimSpace(workDir) == "" {
				workDir = defaultWorkDir
			}
			runReq := ateactor.RunStepRequest{
				StepID:      stepID,
				Name:        stepDisplayName(step.Name, i),
				TargetAddr:  targetAddr,
				Command:     step.Command,
				Args:        step.Args,
				Script:      step.Script,
				WorkingDir:  workDir,
				Env:         stepEnv,
				ResultNames: resultNames,
			}
			st, err = supClient.RunStep(ctx, runReq)
			if err != nil {
				if ateactor.IsStepConflict(err) {
					st, err = supClient.GetStep(ctx, stepID)
				}
				if err != nil {
					return fmt.Errorf("start step %d (%s) on Actor %s/%s: %w", i, runReq.Name, atespace, actorName, err)
				}
			}
		}

		if !st.State.IsTerminal() {
			st, err = supClient.WaitStep(ctx, stepID, actorInlineStepWait)
			if err != nil {
				return fmt.Errorf("poll step %d (%s) on Actor %s/%s: %w", i, stepID, atespace, actorName, err)
			}
		}

		startedAt := now
		if !st.StartedAt.IsZero() {
			startedAt = metav1.Time{Time: st.StartedAt}
		}

		if !st.State.IsTerminal() {
			tr.Status.Steps[i].Waiting = nil
			tr.Status.Steps[i].Running = &corev1.ContainerStateRunning{StartedAt: startedAt}
			return controller.NewRequeueAfter(actorStepPollRequeue)
		}

		upsertTaskRunResults(tr, ts, st.Results)

		finAt := metav1.Time{Time: c.Clock.Now()}
		if st.FinishedAt != nil && !st.FinishedAt.IsZero() {
			finAt = metav1.Time{Time: *st.FinishedAt}
		}
		exitCode := int32(0)
		if st.ExitCode != nil {
			exitCode = int32(*st.ExitCode)
		}

		if st.State == ateactor.StepStateCompleted && exitCode == 0 {
			tr.Status.Steps[i].Waiting = nil
			tr.Status.Steps[i].Running = nil
			tr.Status.Steps[i].TerminationReason = "Completed"
			tr.Status.Steps[i].Terminated = &corev1.ContainerStateTerminated{
				ExitCode:    0,
				Reason:      "Completed",
				StartedAt:   startedAt,
				FinishedAt:  finAt,
				ContainerID: synth.StepContainerImages[i],
			}
			continue
		}

		if step.OnError == v1.Continue {
			tr.Status.Steps[i].Waiting = nil
			tr.Status.Steps[i].Running = nil
			tr.Status.Steps[i].TerminationReason = "Continued"
			tr.Status.Steps[i].Terminated = &corev1.ContainerStateTerminated{
				ExitCode:    exitCode,
				Reason:      "Continued",
				Message:     st.Error,
				StartedAt:   startedAt,
				FinishedAt:  finAt,
				ContainerID: synth.StepContainerImages[i],
			}
			continue
		}

		// Step failed fatally: record terminal state, checkpoint Actor synchronously for rescue/inspection, and mark TaskRun Failed.
		tr.Status.Steps[i].Waiting = nil
		tr.Status.Steps[i].Running = nil
		tr.Status.Steps[i].TerminationReason = "Error"
		tr.Status.Steps[i].Terminated = &corev1.ContainerStateTerminated{
			ExitCode:    exitCode,
			Reason:      "Error",
			Message:     st.Error,
			StartedAt:   startedAt,
			FinishedAt:  finAt,
			ContainerID: synth.StepContainerImages[i],
		}
		for rem := i + 1; rem < len(tr.Status.Steps); rem++ {
			tr.Status.Steps[rem].Waiting = nil
			tr.Status.Steps[rem].Running = nil
			tr.Status.Steps[rem].TerminationReason = "Skipped"
			tr.Status.Steps[rem].Terminated = &corev1.ContainerStateTerminated{
				ExitCode:   0,
				Reason:     "Skipped",
				StartedAt:  finAt,
				FinishedAt: finAt,
			}
		}
		delete(tr.Annotations, ateactor.AnnotationCurrentStepID)

		failureTag := ateactor.FailureTagName(tr.Name)
		if _, tagErr := ateactor.EnsureAtespaceTag(ctx, rt.Control(), atespace, failureTag, actorName); tagErr != nil {
			logger.Warnf("Failed to checkpoint failure Snapshot Tag %s/%s from Actor %s: %v", atespace, failureTag, actorName, tagErr)
			if suspErr := ateactor.SuspendActorSync(ctx, rt.Control(), atespace, actorName); suspErr != nil {
				logger.Warnf("Failed to suspend failed Actor %s/%s: %v", atespace, actorName, suspErr)
			}
		} else {
			tr.Annotations[ateactor.AnnotationFailureTag] = failureTag
			tr.Annotations[ateactor.AnnotationFailedActor] = actorName
			if tr.Status.Annotations == nil {
				tr.Status.Annotations = make(map[string]string)
			}
			tr.Status.Annotations[ateactor.AnnotationFailureTag] = failureTag
			tr.Status.Annotations[ateactor.AnnotationFailedActor] = actorName
		}

		tr.Status.CompletionTime = &finAt
		failMsg := fmt.Sprintf("step %q exited with code %d", stepDisplayName(step.Name, i), exitCode)
		if st.Error != "" {
			failMsg = fmt.Sprintf("%s: %s", failMsg, st.Error)
		}
		failReason := v1.TaskRunReasonFailed
		if tr.Annotations[v1.PipelineTaskOnErrorAnnotation] == string(v1.PipelineTaskContinue) {
			failReason = v1.TaskRunReasonFailureIgnored
		}
		tr.Status.MarkResourceFailed(failReason, errors.New(failMsg))
		tr.Status.CompletionTime = &finAt
		return nil
	}

	delete(tr.Annotations, ateactor.AnnotationCurrentStepID)

	if rtr != nil && rtr.TaskSpec != nil {
		specCopy := rtr.TaskSpec.DeepCopy()
		specCopy.SetDefaults(ctx)
		if err := validateTaskRunResults(tr, specCopy); err != nil {
			tr.Status.MarkResourceFailed(v1.TaskRunReasonFailedValidation, err)
			return controller.NewPermanentError(err)
		}
	}

	doneAt := metav1.Time{Time: c.Clock.Now()}
	for i := range tr.Status.Sidecars {
		started := doneAt
		if tr.Status.Sidecars[i].Running != nil {
			started = tr.Status.Sidecars[i].Running.StartedAt
		}
		tr.Status.Sidecars[i].Running = nil
		tr.Status.Sidecars[i].Waiting = nil
		tr.Status.Sidecars[i].Terminated = &corev1.ContainerStateTerminated{
			ExitCode:   0,
			Reason:     "Completed",
			StartedAt:  started,
			FinishedAt: doneAt,
		}
	}

	retain := tr.Annotations[ateactor.AnnotationRetainWorkspaceActor] == "true"
	if actorMode == ateactor.ActorModeTrunk || (actorMode == ateactor.ActorModeStandalone && retain) {
		if err := ateactor.SuspendActorSync(ctx, rt.Control(), atespace, actorName); err != nil {
			return err
		}
	} else {
		if err := ateactor.DeleteActorIgnoreNotFound(ctx, rt.Control(), atespace, actorName); err != nil {
			return err
		}
	}

	tr.Status.CompletionTime = &doneAt
	tr.Status.SetCondition(&apis.Condition{
		Type:    apis.ConditionSucceeded,
		Status:  corev1.ConditionTrue,
		Reason:  v1.TaskRunReasonSuccessful.String(),
		Message: "All Steps have completed executing",
	})
	return nil
}

func (c *Reconciler) seedActorWorkspaces(
	ctx context.Context,
	tr *v1.TaskRun,
	ts *v1.TaskSpec,
	supClient *ateactor.SupervisorClient,
) error {
	var relDirs []string
	for i := range tr.Spec.Workspaces {
		wb := &tr.Spec.Workspaces[i]
		mountPath := resolveActorWorkspaceMountPath(ts, wb)
		if rel, ok := workspaceRelSubDir(mountPath); ok && rel != "" {
			relDirs = append(relDirs, rel)
		}
	}
	if err := supClient.EnsureWorkspaceDirs(ctx, relDirs...); err != nil {
		return err
	}

	for i := range tr.Spec.Workspaces {
		wb := &tr.Spec.Workspaces[i]
		mountPath := resolveActorWorkspaceMountPath(ts, wb)
		subDir, ok := workspaceRelSubDir(mountPath)
		if !ok {
			subDir = wb.Name
		}

		switch {
		case wb.ConfigMap != nil:
			files, mode, err := c.readConfigMapWorkspaceFiles(ctx, tr.Namespace, wb.ConfigMap)
			if err != nil {
				return err
			}
			if len(files) > 0 {
				if _, err := supClient.UploadFiles(ctx, ateactor.UploadFilesRequest{
					SubDir: subDir,
					Files:  files,
					Mode:   mode,
				}); err != nil {
					return fmt.Errorf("seed ConfigMap workspace %q into Actor: %w", wb.Name, err)
				}
			}
		case wb.Secret != nil:
			files, mode, err := c.readSecretWorkspaceFiles(ctx, tr.Namespace, wb.Secret)
			if err != nil {
				return err
			}
			if len(files) > 0 {
				if _, err := supClient.UploadFiles(ctx, ateactor.UploadFilesRequest{
					SubDir: subDir,
					Files:  files,
					Mode:   mode,
				}); err != nil {
					return fmt.Errorf("seed Secret workspace %q into Actor: %w", wb.Name, err)
				}
			}
		}
	}
	return nil
}

func (c *Reconciler) readConfigMapWorkspaceFiles(ctx context.Context, namespace string, src *corev1.ConfigMapVolumeSource) (map[string][]byte, uint32, error) {
	optional := src.Optional != nil && *src.Optional
	cm, err := c.KubeClientSet.CoreV1().ConfigMaps(namespace).Get(ctx, src.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) && optional {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("get ConfigMap %s/%s: %w", namespace, src.Name, err)
	}
	var mode uint32
	if src.DefaultMode != nil {
		mode = uint32(*src.DefaultMode)
	}
	files := make(map[string][]byte)
	if len(src.Items) > 0 {
		for _, item := range src.Items {
			if val, ok := cm.Data[item.Key]; ok {
				files[item.Path] = []byte(val)
			} else if bin, ok := cm.BinaryData[item.Key]; ok {
				files[item.Path] = append([]byte(nil), bin...)
			} else if !optional {
				return nil, 0, fmt.Errorf("ConfigMap %s/%s missing key %q", namespace, src.Name, item.Key)
			}
		}
		return files, mode, nil
	}
	for k, v := range cm.Data {
		files[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		files[k] = append([]byte(nil), v...)
	}
	return files, mode, nil
}

func (c *Reconciler) readSecretWorkspaceFiles(ctx context.Context, namespace string, src *corev1.SecretVolumeSource) (map[string][]byte, uint32, error) {
	optional := src.Optional != nil && *src.Optional
	sec, err := c.KubeClientSet.CoreV1().Secrets(namespace).Get(ctx, src.SecretName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) && optional {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("get Secret %s/%s: %w", namespace, src.SecretName, err)
	}
	var mode uint32
	if src.DefaultMode != nil {
		mode = uint32(*src.DefaultMode)
	}
	files := make(map[string][]byte)
	if len(src.Items) > 0 {
		for _, item := range src.Items {
			if val, ok := sec.Data[item.Key]; ok {
				files[item.Path] = append([]byte(nil), val...)
			} else if sVal, ok := sec.StringData[item.Key]; ok {
				files[item.Path] = []byte(sVal)
			} else if !optional {
				return nil, 0, fmt.Errorf("Secret %s/%s missing key %q", namespace, src.SecretName, item.Key)
			}
		}
		return files, mode, nil
	}
	for k, v := range sec.Data {
		files[k] = append([]byte(nil), v...)
	}
	for k, v := range sec.StringData {
		files[k] = []byte(v)
	}
	return files, mode, nil
}

func (c *Reconciler) resolveEnvVars(ctx context.Context, tr *v1.TaskRun, envVars []corev1.EnvVar) ([]ateactor.EnvEntry, error) {
	if len(envVars) == 0 {
		return nil, nil
	}
	out := make([]ateactor.EnvEntry, 0, len(envVars))
	for _, ev := range envVars {
		val := ev.Value
		if ev.ValueFrom != nil {
			switch {
			case ev.ValueFrom.FieldRef != nil:
				switch ev.ValueFrom.FieldRef.FieldPath {
				case "metadata.name":
					val = tr.Name
				case "metadata.namespace":
					val = tr.Namespace
				case "metadata.uid":
					val = string(tr.UID)
				default:
					if strings.HasPrefix(ev.ValueFrom.FieldRef.FieldPath, "metadata.labels['") {
						k := strings.TrimSuffix(strings.TrimPrefix(ev.ValueFrom.FieldRef.FieldPath, "metadata.labels['"), "']")
						val = tr.Labels[k]
					} else if strings.HasPrefix(ev.ValueFrom.FieldRef.FieldPath, "metadata.annotations['") {
						k := strings.TrimSuffix(strings.TrimPrefix(ev.ValueFrom.FieldRef.FieldPath, "metadata.annotations['"), "']")
						val = tr.Annotations[k]
					}
				}
			case ev.ValueFrom.ConfigMapKeyRef != nil:
				ref := ev.ValueFrom.ConfigMapKeyRef
				opt := ref.Optional != nil && *ref.Optional
				cm, err := c.KubeClientSet.CoreV1().ConfigMaps(tr.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					if k8serrors.IsNotFound(err) && opt {
						continue
					}
					return nil, fmt.Errorf("resolve env %q ConfigMap %q: %w", ev.Name, ref.Name, err)
				}
				v, exists := cm.Data[ref.Key]
				if !exists && !opt {
					return nil, fmt.Errorf("ConfigMap %q missing key %q for env %q", ref.Name, ref.Key, ev.Name)
				}
				val = v
			case ev.ValueFrom.SecretKeyRef != nil:
				ref := ev.ValueFrom.SecretKeyRef
				opt := ref.Optional != nil && *ref.Optional
				sec, err := c.KubeClientSet.CoreV1().Secrets(tr.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					if k8serrors.IsNotFound(err) && opt {
						continue
					}
					return nil, fmt.Errorf("resolve env %q Secret %q: %w", ev.Name, ref.Name, err)
				}
				if raw, exists := sec.Data[ref.Key]; exists {
					val = string(raw)
				} else if sVal, exists := sec.StringData[ref.Key]; exists {
					val = sVal
				} else if !opt {
					return nil, fmt.Errorf("Secret %q missing key %q for env %q", ref.Name, ref.Key, ev.Name)
				}
			}
		}
		out = append(out, ateactor.EnvEntry{Name: ev.Name, Value: val})
	}
	return out, nil
}

func ensureStepAndSidecarStatuses(
	tr *v1.TaskRun,
	steps []v1.Step,
	sidecars []v1.Sidecar,
	synth *ateactor.SynthesizedTemplate,
	now metav1.Time,
) {
	if len(tr.Status.Steps) != len(steps) {
		tr.Status.Steps = make([]v1.StepState, len(steps))
	}
	for i, s := range steps {
		st := tr.Status.Steps[i]
		st.Name = stepDisplayName(s.Name, i)
		st.Container = fmt.Sprintf("c%d", synth.StepContainerIndices[i])
		st.ImageID = synth.StepContainerImages[i]
		if st.Waiting == nil && st.Running == nil && st.Terminated == nil {
			st.Waiting = &corev1.ContainerStateWaiting{Reason: "PodInitializing"}
		}
		tr.Status.Steps[i] = st
	}
	if len(tr.Status.Sidecars) != len(sidecars) {
		tr.Status.Sidecars = make([]v1.SidecarState, len(sidecars))
	}
	for i, sc := range sidecars {
		var img string
		ctrIdx := len(synth.Template.GetContainers()) - len(sidecars) + i
		if ctrIdx >= 0 && ctrIdx < len(synth.Template.GetContainers()) {
			img = synth.Template.GetContainers()[ctrIdx].GetImage()
		}
		st := tr.Status.Sidecars[i]
		st.Name = sc.Name
		st.Container = fmt.Sprintf("s%d", i)
		st.ImageID = img
		if st.Waiting == nil && st.Running == nil && st.Terminated == nil {
			st.Running = &corev1.ContainerStateRunning{StartedAt: now}
		}
		tr.Status.Sidecars[i] = st
	}
}

func collectDeclaredResultNames(ts *v1.TaskSpec) []string {
	seen := make(map[string]struct{}, len(ts.Results))
	var names []string
	for _, r := range ts.Results {
		if r.Name == "" {
			continue
		}
		if _, dup := seen[r.Name]; !dup {
			seen[r.Name] = struct{}{}
			names = append(names, r.Name)
		}
	}
	for _, s := range ts.Steps {
		for _, r := range s.Results {
			if r.Name == "" {
				continue
			}
			if _, dup := seen[r.Name]; !dup {
				seen[r.Name] = struct{}{}
				names = append(names, r.Name)
			}
		}
	}
	return names
}

func upsertTaskRunResults(tr *v1.TaskRun, ts *v1.TaskSpec, emitted map[string]string) {
	if len(emitted) == 0 {
		return
	}
	declaredType := make(map[string]v1.ResultsType, len(ts.Results))
	for _, r := range ts.Results {
		declaredType[r.Name] = r.Type
	}
	for _, s := range ts.Steps {
		for _, r := range s.Results {
			if _, ok := declaredType[r.Name]; !ok {
				declaredType[r.Name] = r.Type
			}
		}
	}

	byName := make(map[string]v1.TaskRunResult, len(tr.Status.Results)+len(emitted))
	var order []string
	for _, existing := range tr.Status.Results {
		byName[existing.Name] = existing
		order = append(order, existing.Name)
	}

	keys := make([]string, 0, len(emitted))
	for k := range emitted {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		rawVal := emitted[k]
		rType, declared := declaredType[k]
		if !declared {
			continue
		}
		var trr v1.TaskRunResult
		if rType == "" || rType == v1.ResultsTypeString {
			trr = v1.TaskRunResult{
				Name:  k,
				Type:  v1.ResultsTypeString,
				Value: *v1.NewStructuredValues(rawVal),
			}
		} else {
			var parsed v1.ResultValue
			if err := parsed.UnmarshalJSON([]byte(rawVal)); err != nil {
				trr = v1.TaskRunResult{
					Name:  k,
					Type:  rType,
					Value: *v1.NewStructuredValues(rawVal),
				}
			} else {
				trr = v1.TaskRunResult{
					Name:  k,
					Type:  v1.ResultsType(parsed.Type),
					Value: parsed,
				}
			}
		}
		if _, exists := byName[k]; !exists {
			order = append(order, k)
		}
		byName[k] = trr
	}

	tr.Status.Results = make([]v1.TaskRunResult, 0, len(order))
	for _, name := range order {
		tr.Status.Results = append(tr.Status.Results, byName[name])
	}
}

func resolveActorWorkspaceMountPath(ts *v1.TaskSpec, wb *v1.WorkspaceBinding) string {
	mountPath := filepath.Join(pipeline.WorkspaceDir, wb.Name)
	if ts != nil {
		for _, decl := range ts.Workspaces {
			if decl.Name == wb.Name {
				mountPath = decl.GetMountPath()
				break
			}
		}
	}
	if wb.SubPath != "" {
		mountPath = filepath.Join(mountPath, wb.SubPath)
	}
	return mountPath
}

func workspaceRelSubDir(mountPath string) (string, bool) {
	cleaned := filepath.Clean(mountPath)
	if cleaned == ateactor.MountPathWorkspace {
		return "", true
	}
	prefix := ateactor.MountPathWorkspace + "/"
	if strings.HasPrefix(cleaned, prefix) {
		return strings.TrimPrefix(cleaned, prefix), true
	}
	return "", false
}

func actorStepID(tr *v1.TaskRun, idx int) string {
	base := string(tr.UID)
	if base == "" {
		base = tr.Namespace + "-" + tr.Name
	}
	var b strings.Builder
	for i := 0; i < len(base); i++ {
		ch := base[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			b.WriteByte(ch)
		} else {
			b.WriteByte('-')
		}
	}
	id := fmt.Sprintf("%s-r%d-step-%d", strings.Trim(b.String(), "-"), len(tr.Status.RetriesStatus), idx)
	if len(id) > 120 {
		id = id[len(id)-120:]
	}
	return id
}

func stepDisplayName(name string, idx int) string {
	return podconvert.TrimStepPrefix(podconvert.StepName(name, idx))
}

func extractResourceLimits(reqs corev1.ResourceRequirements) (cpuMilli int64, memBytes int64) {
	if reqs.Limits == nil {
		return 0, 0
	}
	if cpu, ok := reqs.Limits[corev1.ResourceCPU]; ok {
		cpuMilli = cpu.MilliValue()
	}
	if mem, ok := reqs.Limits[corev1.ResourceMemory]; ok {
		memBytes = mem.Value()
	}
	return cpuMilli, memBytes
}
