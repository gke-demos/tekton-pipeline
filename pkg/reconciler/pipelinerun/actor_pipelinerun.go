package pipelinerun

import (
	"context"
	"sort"
	"strings"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"github.com/tektoncd/pipeline/pkg/reconciler/pipelinerun/resources"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actorTaskRunPlan struct {
	Mode      string
	ActorName string
	SourceTag string
}

type actorPlanContextKey struct{}

func withActorTaskRunPlans(ctx context.Context, plans map[string]actorTaskRunPlan) context.Context {
	if len(plans) == 0 {
		return ctx
	}
	return context.WithValue(ctx, actorPlanContextKey{}, plans)
}

func actorTaskRunPlanFromContext(ctx context.Context, taskRunName string) (actorTaskRunPlan, bool) {
	if ctx == nil {
		return actorTaskRunPlan{}, false
	}
	plans, ok := ctx.Value(actorPlanContextKey{}).(map[string]actorTaskRunPlan)
	if !ok || len(plans) == 0 {
		return actorTaskRunPlan{}, false
	}
	p, exists := plans[taskRunName]
	return p, exists
}

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

func hasPVCOrClaimTemplateWorkspace(wbs []v1.WorkspaceBinding) bool {
	for i := range wbs {
		if wbs[i].PersistentVolumeClaim != nil || wbs[i].VolumeClaimTemplate != nil {
			return true
		}
	}
	return false
}

func rptUsesActorWorkspace(pr *v1.PipelineRun, rpt *resources.ResolvedPipelineTask) bool {
	if pr == nil || rpt == nil || rpt.PipelineTask == nil || rpt.IsCustomTask() || rpt.IsChildPipeline() {
		return false
	}
	actorWorkspaces := make(map[string]struct{})
	for i := range pr.Spec.Workspaces {
		if pr.Spec.Workspaces[i].Actor != nil {
			actorWorkspaces[pr.Spec.Workspaces[i].Name] = struct{}{}
		}
	}
	if len(actorWorkspaces) == 0 {
		return false
	}
	for _, ws := range rpt.PipelineTask.Workspaces {
		parentName := ws.Workspace
		if parentName == "" {
			parentName = ws.Name
		}
		if _, ok := actorWorkspaces[parentName]; ok {
			return true
		}
	}
	return false
}

func applyActorSkipTasks(pr *v1.PipelineRun, facts *resources.PipelineRunFacts) {
	if pr == nil || facts == nil {
		return
	}
	wb, ok := findActorWorkspace(pr.Spec.Workspaces)
	if !ok || wb.Actor == nil || len(wb.Actor.SkipTasks) == 0 {
		return
	}
	skipSet := make(map[string]struct{}, len(wb.Actor.SkipTasks))
	for _, name := range wb.Actor.SkipTasks {
		trimmed := strings.TrimSpace(name)
		if trimmed != "" {
			skipSet[trimmed] = struct{}{}
		}
	}
	if len(skipSet) == 0 {
		return
	}
	changed := false
	for _, rpt := range facts.State {
		if rpt == nil || rpt.PipelineTask == nil {
			continue
		}
		if _, shouldSkip := skipSet[rpt.PipelineTask.Name]; shouldSkip {
			if len(rpt.TaskRuns) == 0 && len(rpt.CustomRuns) == 0 && len(rpt.ChildPipelineRuns) == 0 {
				rpt.SkipFromSourceTag = true
				changed = true
			}
		}
	}
	if changed {
		facts.ResetSkippedCache()
	}
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

// ensurePipelineRunActorMetadata assigns the deterministic trunk Workspace Actor name
// (`ws-<pr.Name>`) and records `tekton.ate.dev/workspace-actor` and `tekton.ate.dev/atespace`
// on the PipelineRun's annotations whenever an Actor workspace binding is present.
func ensurePipelineRunActorMetadata(pr *v1.PipelineRun) (atespace, actorName string, ok bool) {
	wb, found := findActorWorkspace(pr.Spec.Workspaces)
	if !found || wb.Actor == nil {
		return "", "", false
	}
	atespace = strings.TrimSpace(wb.Actor.Atespace)
	if pr.Annotations == nil {
		pr.Annotations = make(map[string]string)
	}
	actorName = pr.Annotations[ateactor.AnnotationWorkspaceActor]
	if actorName == "" {
		actorName = ateactor.WorkspaceActorName(pr.Name)
		pr.Annotations[ateactor.AnnotationWorkspaceActor] = actorName
	}
	if atespace != "" {
		pr.Annotations[ateactor.AnnotationAtespace] = atespace
	}
	if srcTag := strings.TrimSpace(wb.Actor.SourceTag); srcTag != "" && pr.Annotations[ateactor.AnnotationSourceTag] == "" {
		pr.Annotations[ateactor.AnnotationSourceTag] = srcTag
	}
	return atespace, actorName, true
}

// planNextActorTaskRuns inspects the current DAG scheduling wave (nextRpts) and determines
// whether Actor-backed TaskRuns execute sequentially on the trunk Workspace Actor or fan out
// into isolated copy-on-write Branch Actors seeded from a milestone Snapshot Tag.
func (c *Reconciler) planNextActorTaskRuns(
	ctx context.Context,
	pr *v1.PipelineRun,
	facts *resources.PipelineRunFacts,
	nextRpts resources.PipelineRunState,
) (map[string]actorTaskRunPlan, error) {
	wb, found := findActorWorkspace(pr.Spec.Workspaces)
	if !found || wb.Actor == nil {
		return nil, nil
	}
	atespace, trunkActorName, _ := ensurePipelineRunActorMetadata(pr)
	if atespace == "" {
		return nil, nil
	}

	var readyActorRpts []*resources.ResolvedPipelineTask
	totalReadyActorTaskRuns := 0
	var waveTaskNames []string

	for _, rpt := range nextRpts {
		if rpt == nil || rpt.Skip(facts).IsSkipped || rpt.IsFinallySkipped(facts).IsSkipped {
			continue
		}
		if !rptUsesActorWorkspace(pr, rpt) {
			continue
		}
		readyActorRpts = append(readyActorRpts, rpt)
		totalReadyActorTaskRuns += len(rpt.TaskRunNames)
		waveTaskNames = append(waveTaskNames, rpt.PipelineTask.Name)
	}
	if len(readyActorRpts) == 0 || totalReadyActorTaskRuns == 0 {
		return nil, nil
	}

	runningActorTasks := 0
	if facts != nil {
		for _, rpt := range facts.State {
			if rptUsesActorWorkspace(pr, rpt) && rpt.IsRunning() {
				runningActorTasks++
			}
		}
	}

	plans := make(map[string]actorTaskRunPlan, totalReadyActorTaskRuns)
	initialSourceTag := strings.TrimSpace(wb.Actor.SourceTag)

	// Single sequential or fan-in task with no concurrent Actor task running -> trunk mode.
	if totalReadyActorTaskRuns == 1 && runningActorTasks == 0 {
		trName := readyActorRpts[0].TaskRunNames[0]
		plans[trName] = actorTaskRunPlan{
			Mode:      ateactor.ActorModeTrunk,
			ActorName: trunkActorName,
			SourceTag: initialSourceTag,
		}
		return plans, nil
	}

	// Parallel DAG fan-out or matrix fan-out -> checkpoint trunk Workspace Actor, mint milestone
	// Snapshot Tag, and schedule isolated copy-on-write Branch Actors.
	sort.Strings(waveTaskNames)
	waveKey := strings.Join(waveTaskNames, "-")

	rt, err := c.getActorRuntime(ctx)
	if err != nil {
		return nil, err
	}

	var milestoneTag string
	_, getErr := rt.Control().GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: trunkActorName},
	})
	switch {
	case getErr == nil:
		milestoneTag = ateactor.MilestoneTagName(pr.Name, waveKey)
		if _, err := ateactor.EnsureAtespaceTag(ctx, rt.Control(), atespace, milestoneTag, trunkActorName); err != nil {
			return nil, err
		}
	case status.Code(getErr) == codes.NotFound:
		// Trunk Workspace Actor has not executed any task yet (e.g. root fan-out or mid-DAG resume
		// with skipTasks); fork branches directly from initialSourceTag if configured.
		milestoneTag = initialSourceTag
	default:
		return nil, getErr
	}

	if milestoneTag != "" {
		if pr.Annotations == nil {
			pr.Annotations = make(map[string]string)
		}
		pr.Annotations[ateactor.AnnotationMilestoneTag] = milestoneTag
	}

	for _, rpt := range readyActorRpts {
		for _, trName := range rpt.TaskRunNames {
			plans[trName] = actorTaskRunPlan{
				Mode:      ateactor.ActorModeBranch,
				ActorName: ateactor.BranchActorName(trName),
				SourceTag: milestoneTag,
			}
		}
	}
	return plans, nil
}

// annotateChildActorTaskRun propagates trunk or branch Actor identity onto child TaskRuns
// that bind an Actor workspace.
func annotateChildActorTaskRun(ctx context.Context, pr *v1.PipelineRun, tr *v1.TaskRun) {
	wb, found := findActorWorkspace(tr.Spec.Workspaces)
	if !found || wb.Actor == nil {
		return
	}
	if tr.Annotations == nil {
		tr.Annotations = make(map[string]string)
	}
	atespace := strings.TrimSpace(wb.Actor.Atespace)
	actorName := pr.Annotations[ateactor.AnnotationWorkspaceActor]
	if actorName == "" {
		actorName = ateactor.WorkspaceActorName(pr.Name)
	}

	if plan, ok := actorTaskRunPlanFromContext(ctx, tr.Name); ok {
		if plan.Mode != "" {
			tr.Annotations[ateactor.AnnotationActorMode] = plan.Mode
		}
		if plan.ActorName != "" {
			tr.Annotations[ateactor.AnnotationActorName] = plan.ActorName
		}
		if plan.SourceTag != "" {
			tr.Annotations[ateactor.AnnotationSourceTag] = plan.SourceTag
		}
	}

	if tr.Annotations[ateactor.AnnotationActorMode] == "" {
		tr.Annotations[ateactor.AnnotationActorMode] = ateactor.ActorModeTrunk
	}
	if tr.Annotations[ateactor.AnnotationActorName] == "" {
		tr.Annotations[ateactor.AnnotationActorName] = actorName
	}
	if tr.Annotations[ateactor.AnnotationSourceTag] == "" {
		if srcTag := strings.TrimSpace(wb.Actor.SourceTag); srcTag != "" {
			tr.Annotations[ateactor.AnnotationSourceTag] = srcTag
		}
	}
	if atespace != "" {
		tr.Annotations[ateactor.AnnotationAtespace] = atespace
	}
}

// cleanupActorWorkspace deletes the trunk Workspace Actor when the PipelineRun finishes
// successfully (or is cancelled) unless `tekton.ate.dev/retain-workspace-actor == "true"` is set.
// Failed PipelineRuns preserve the suspended Workspace Actor so it can be snapshotted or rescued.
func (c *Reconciler) cleanupActorWorkspace(ctx context.Context, pr *v1.PipelineRun) error {
	wb, found := findActorWorkspace(pr.Spec.Workspaces)
	if !found || wb.Actor == nil {
		return nil
	}
	if pr.Annotations[ateactor.AnnotationRetainWorkspaceActor] == "true" {
		return nil
	}
	if !pr.IsSuccessful() && !pr.IsCancelled() {
		return nil
	}
	actorName := pr.Annotations[ateactor.AnnotationWorkspaceActor]
	if actorName == "" {
		return nil
	}
	atespace := strings.TrimSpace(wb.Actor.Atespace)
	if atespace == "" {
		atespace = strings.TrimSpace(pr.Annotations[ateactor.AnnotationAtespace])
	}
	if atespace == "" {
		return nil
	}
	rt, err := c.getActorRuntime(ctx)
	if err != nil {
		return err
	}
	return ateactor.DeleteActorIgnoreNotFound(ctx, rt.Control(), atespace, actorName)
}
