package pipelinerun

import (
	"context"
	"strings"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
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

func hasPVCOrClaimTemplateWorkspace(wbs []v1.WorkspaceBinding) bool {
	for i := range wbs {
		if wbs[i].PersistentVolumeClaim != nil || wbs[i].VolumeClaimTemplate != nil {
			return true
		}
	}
	return false
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
	return atespace, actorName, true
}

// annotateChildActorTaskRun propagates trunk Workspace Actor identity onto child TaskRuns
// that bind an Actor workspace.
func annotateChildActorTaskRun(pr *v1.PipelineRun, tr *v1.TaskRun) {
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
	if tr.Annotations[ateactor.AnnotationActorMode] == "" {
		tr.Annotations[ateactor.AnnotationActorMode] = ateactor.ActorModeTrunk
	}
	if tr.Annotations[ateactor.AnnotationActorName] == "" {
		tr.Annotations[ateactor.AnnotationActorName] = actorName
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
