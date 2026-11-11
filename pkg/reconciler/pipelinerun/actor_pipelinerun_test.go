package pipelinerun

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tektoncd/pipeline/pkg/apis/config"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor/fakecontrol"
	"github.com/tektoncd/pipeline/pkg/reconciler/taskrun"
	th "github.com/tektoncd/pipeline/pkg/reconciler/testing"
	_ "github.com/tektoncd/pipeline/pkg/taskrunmetrics/fake"
	"github.com/tektoncd/pipeline/pkg/workspace"
	"github.com/tektoncd/pipeline/test"
	"go.ate.dev/substrate/api/ateapi/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	cminformer "knative.dev/pkg/configmap/informer"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"
	"sigs.k8s.io/yaml"
)

const (
	actorPRTestSupImg    = "gcr.io/tekton-actors/step-supervisor@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	actorPRTestGolangImg = "golang@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	actorPRTestAlpineImg = "alpine@sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func setupPipelineAndTaskRunControllers(t *testing.T, d test.Data) (context.Context, test.Clients, test.Informers, *controller.Impl, *controller.Impl, func()) {
	t.Helper()
	ctx, _ := th.SetupFakeContext(t)
	ctx, cancel := context.WithCancel(ctx)
	test.EnsureConfigurationConfigMapsExist(&d)
	c, informers := test.SeedTestData(t, ctx, d)
	configMapWatcher := cminformer.NewInformedWatcher(c.Kube, system.Namespace())
	opts := pipeline.Options{Images: images}

	prCtl := NewController(&opts, testClock)(ctx, configMapWatcher)
	if la, ok := prCtl.Reconciler.(reconciler.LeaderAware); ok {
		if err := la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {}); err != nil {
			t.Fatalf("promote PipelineRun leader: %v", err)
		}
	}
	trCtl := taskrun.NewController(&opts, testClock)(ctx, configMapWatcher)
	if la, ok := trCtl.Reconciler.(reconciler.LeaderAware); ok {
		if err := la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {}); err != nil {
			t.Fatalf("promote TaskRun leader: %v", err)
		}
	}
	if err := configMapWatcher.Start(ctx.Done()); err != nil {
		t.Fatalf("start configmap watcher: %v", err)
	}
	return ctx, c, informers, prCtl, trCtl, cancel
}

func syncPipelineAndTaskInformers(t *testing.T, ctx context.Context, ns string, c test.Clients, informers test.Informers) {
	t.Helper()
	prs, err := c.Pipeline.TektonV1().PipelineRuns(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list PipelineRuns: %v", err)
	}
	for i := range prs.Items {
		_ = informers.PipelineRun.Informer().GetIndexer().Add(&prs.Items[i])
		_ = informers.PipelineRun.Informer().GetIndexer().Update(&prs.Items[i])
	}
	trs, err := c.Pipeline.TektonV1().TaskRuns(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list TaskRuns: %v", err)
	}
	for i := range trs.Items {
		_ = informers.TaskRun.Informer().GetIndexer().Add(&trs.Items[i])
		_ = informers.TaskRun.Informer().GetIndexer().Update(&trs.Items[i])
	}
}

func TestActorPipelineRun_SequentialTwoTasksReuseWorkspaceActorAndZeroPVCs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		coschedule   string
		retainActor  bool
		expectDelete bool
	}{
		{
			name:         "coschedule workspaces deletes workspace actor on completion",
			coschedule:   config.CoscheduleWorkspaces,
			retainActor:  false,
			expectDelete: true,
		},
		{
			name:         "coschedule pipelineruns retains workspace actor when annotated",
			coschedule:   config.CoschedulePipelineRuns,
			retainActor:  true,
			expectDelete: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taskA := &v1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-a", Namespace: "foo"},
				Spec: v1.TaskSpec{
					Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
					Results:    []v1.TaskResult{{Name: "commit"}},
					Steps: []v1.Step{{
						Name:  "clone-and-build",
						Image: "golang:1.23",
						Script: `#!/bin/sh
set -eu
printf "built-by-task-a" > $(workspaces.source.path)/artifact.txt
printf "abc123456789" > $(results.commit.path)
`,
					}},
				},
			}
			taskB := &v1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "task-b", Namespace: "foo"},
				Spec: v1.TaskSpec{
					Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
					Params:     []v1.ParamSpec{{Name: "commit", Type: v1.ParamTypeString}},
					Results:    []v1.TaskResult{{Name: "summary"}},
					Steps: []v1.Step{{
						Name:  "verify-and-package",
						Image: "alpine:3.20",
						Script: `#!/bin/sh
set -eu
got="$(cat $(workspaces.source.path)/artifact.txt)"
if [ "$got" != "built-by-task-a" ]; then
  echo "unexpected artifact: $got" >&2
  exit 1
fi
printf "verified:$(params.commit)" > $(workspaces.source.path)/verified.txt
printf "verified:$(params.commit)" > $(results.summary.path)
`,
					}},
				},
			}

			p := &v1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{Name: "seq-pipeline", Namespace: "foo"},
				Spec: v1.PipelineSpec{
					Workspaces: []v1.PipelineWorkspaceDeclaration{{Name: "shared"}},
					Results: []v1.PipelineResult{{
						Name:  "final-summary",
						Value: *v1.NewStructuredValues("$(tasks.task-b.results.summary)"),
					}},
					Tasks: []v1.PipelineTask{
						{
							Name:       "task-a",
							TaskRef:    &v1.TaskRef{Name: "task-a"},
							Workspaces: []v1.WorkspacePipelineTaskBinding{{Name: "source", Workspace: "shared"}},
						},
						{
							Name:     "task-b",
							TaskRef:  &v1.TaskRef{Name: "task-b"},
							RunAfter: []string{"task-a"},
							Params: []v1.Param{{
								Name:  "commit",
								Value: *v1.NewStructuredValues("$(tasks.task-a.results.commit)"),
							}},
							Workspaces: []v1.WorkspacePipelineTaskBinding{{Name: "source", Workspace: "shared"}},
						},
					},
				},
			}

			prAnnotations := map[string]string{}
			if tc.retainActor {
				prAnnotations[ateactor.AnnotationRetainWorkspaceActor] = "true"
			}
			pr := &v1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "seq-release",
					Namespace:   "foo",
					UID:         "uid-pr-seq-1",
					Annotations: prAnnotations,
				},
				Spec: v1.PipelineRunSpec{
					PipelineRef: &v1.PipelineRef{Name: "seq-pipeline"},
					Workspaces: []v1.WorkspaceBinding{{
						Name: "shared",
						Actor: &v1.ActorWorkspaceSource{
							Atespace:     "ci",
							EgressSecret: "git-creds/token",
						},
					}},
				},
			}

			featureFlagsCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: config.GetFeatureFlagsConfigName(), Namespace: system.Namespace()},
				Data: map[string]string{
					"coschedule": tc.coschedule,
				},
			}

			d := test.Data{
				Pipelines:    []*v1.Pipeline{p},
				PipelineRuns: []*v1.PipelineRun{pr},
				Tasks:        []*v1.Task{taskA, taskB},
				ConfigMaps:   []*corev1.ConfigMap{featureFlagsCM},
				ServiceAccounts: []*corev1.ServiceAccount{{
					ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
				}},
			}

			rt := fakecontrol.NewTestRuntime(t, actorPRTestSupImg, map[string]string{
				"golang:1.23": actorPRTestGolangImg,
				"alpine:3.20": actorPRTestAlpineImg,
			})

			baseCtx, clients, informers, prCtl, trCtl, cancel := setupPipelineAndTaskRunControllers(t, d)
			defer cancel()
			ctx := ateactor.WithRuntime(baseCtx, rt)

			for round := 0; round < 10; round++ {
				if err := prCtl.Reconciler.Reconcile(ctx, "foo/seq-release"); err != nil {
					if ok, _ := controller.IsRequeueKey(err); !ok {
						t.Fatalf("PipelineRun reconcile round %d error: %v", round, err)
					}
				}
				syncPipelineAndTaskInformers(t, ctx, "foo", clients, informers)

				trs, err := clients.Pipeline.TektonV1().TaskRuns("foo").List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Fatalf("list TaskRuns: %v", err)
				}
				for _, childTR := range trs.Items {
					if childTR.IsDone() {
						continue
					}
					if err := trCtl.Reconciler.Reconcile(ctx, "foo/"+childTR.Name); err != nil {
						if ok, _ := controller.IsRequeueKey(err); !ok {
							t.Fatalf("TaskRun %s reconcile error: %v", childTR.Name, err)
						}
					}
					syncPipelineAndTaskInformers(t, ctx, "foo", clients, informers)
				}

				currentPR, err := clients.Pipeline.TektonV1().PipelineRuns("foo").Get(ctx, "seq-release", metav1.GetOptions{})
				if err != nil {
					t.Fatalf("get PipelineRun: %v", err)
				}
				if currentPR.IsDone() {
					pr = currentPR
					break
				}
			}

			cond := pr.Status.GetCondition(apis.ConditionSucceeded)
			if cond == nil || !cond.IsTrue() {
				t.Fatalf("expected PipelineRun Succeeded=True, got %+v", cond)
			}

			if pr.Annotations[ateactor.AnnotationWorkspaceActor] != "ws-seq-release" {
				t.Fatalf("pr workspace-actor annotation = %q, want ws-seq-release", pr.Annotations[ateactor.AnnotationWorkspaceActor])
			}
			if pr.Annotations[ateactor.AnnotationAtespace] != "ci" {
				t.Fatalf("pr atespace annotation = %q, want ci", pr.Annotations[ateactor.AnnotationAtespace])
			}

			// Verify PipelineResult propagated $(tasks.task-a.results.commit) -> task-b -> PipelineRun.Status.Results.
			if len(pr.Status.Results) != 1 || pr.Status.Results[0].Value.StringVal != "verified:abc123456789" {
				t.Fatalf("unexpected PipelineRun results: %+v", pr.Status.Results)
			}

			// Verify child TaskRuns carried trunk Actor annotations and NO AffinityAssistant annotation.
			trs, err := clients.Pipeline.TektonV1().TaskRuns("foo").List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("list TaskRuns: %v", err)
			}
			if len(trs.Items) != 2 {
				t.Fatalf("expected 2 child TaskRuns, got %d", len(trs.Items))
			}
			for _, childTR := range trs.Items {
				if childTR.Annotations[ateactor.AnnotationActorMode] != ateactor.ActorModeTrunk {
					t.Fatalf("TaskRun %s actor-mode = %q, want trunk", childTR.Name, childTR.Annotations[ateactor.AnnotationActorMode])
				}
				if childTR.Annotations[ateactor.AnnotationActorName] != "ws-seq-release" {
					t.Fatalf("TaskRun %s actor-name = %q, want ws-seq-release", childTR.Name, childTR.Annotations[ateactor.AnnotationActorName])
				}
				if aa := childTR.Annotations[workspace.AnnotationAffinityAssistantName]; aa != "" {
					t.Fatalf("expected empty AffinityAssistant annotation on Actor TaskRun %s, got %q", childTR.Name, aa)
				}
			}

			// Verify task-b reused the SAME trunk Workspace Actor and triggered UpdateActor once to repoint ActorTemplate!
			if gotUpdates := rt.Server.ActorUpdateCount("ci", "ws-seq-release"); gotUpdates != 1 {
				t.Fatalf("expected ActorUpdateCount = 1 (golang -> alpine template repoint), got %d", gotUpdates)
			}

			// Verify ZERO PVCs, ZERO AffinityAssistant StatefulSets, and ZERO Pods were created!
			pvcs, err := clients.Kube.CoreV1().PersistentVolumeClaims("foo").List(ctx, metav1.ListOptions{})
			if err != nil || len(pvcs.Items) != 0 {
				t.Fatalf("expected 0 PVCs, got %d (err=%v)", len(pvcs.Items), err)
			}
			sts, err := clients.Kube.AppsV1().StatefulSets("foo").List(ctx, metav1.ListOptions{})
			if err != nil || len(sts.Items) != 0 {
				t.Fatalf("expected 0 AffinityAssistant StatefulSets, got %d (err=%v)", len(sts.Items), err)
			}
			pods, err := clients.Kube.CoreV1().Pods("foo").List(ctx, metav1.ListOptions{})
			if err != nil || len(pods.Items) != 0 {
				t.Fatalf("expected 0 Pods, got %d (err=%v)", len(pods.Items), err)
			}

			if tc.expectDelete {
				deleted := rt.Server.DeletedActors()
				if len(deleted) != 1 || deleted[0] != "ci/ws-seq-release" {
					t.Fatalf("expected trunk Workspace Actor ci/ws-seq-release to be deleted, got %v", deleted)
				}
			} else {
				actor, err := rt.Control().GetActor(ctx, &ateapi.GetActorRequest{
					Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-seq-release"},
				})
				if err != nil {
					t.Fatalf("expected retained Workspace Actor to exist: %v", err)
				}
				if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
					t.Fatalf("expected retained Workspace Actor SUSPENDED, got %v", actor.GetStatus().GetState())
				}
			}
		})
	}
}

func TestStockPipelineRun_WithVolumeClaimTemplateCreatesPVCAndStatefulSet(t *testing.T) {
	task := &v1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-task", Namespace: "foo"},
		Spec: v1.TaskSpec{
			Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
			Steps:      []v1.Step{{Name: "echo", Image: "alpine:3.20", Command: []string{"/bin/echo", "ok"}}},
		},
	}
	p := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-pipeline", Namespace: "foo"},
		Spec: v1.PipelineSpec{
			Workspaces: []v1.PipelineWorkspaceDeclaration{{Name: "shared"}},
			Tasks: []v1.PipelineTask{{
				Name:       "t1",
				TaskRef:    &v1.TaskRef{Name: "pvc-task"},
				Workspaces: []v1.WorkspacePipelineTaskBinding{{Name: "source", Workspace: "shared"}},
			}},
		},
	}
	pr := &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "stock-pvc-pr", Namespace: "foo", UID: "uid-stock-pvc"},
		Spec: v1.PipelineRunSpec{
			PipelineRef: &v1.PipelineRef{Name: "pvc-pipeline"},
			Workspaces: []v1.WorkspaceBinding{{
				Name: "shared",
				VolumeClaimTemplate: &corev1.PersistentVolumeClaim{
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
						},
					},
				},
			}},
		},
	}

	d := test.Data{
		Pipelines:    []*v1.Pipeline{p},
		PipelineRuns: []*v1.PipelineRun{pr},
		Tasks:        []*v1.Task{task},
		ServiceAccounts: []*corev1.ServiceAccount{{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
		}},
	}

	testAssets, cancel := getPipelineRunController(t, d)
	defer cancel()

	if err := testAssets.Controller.Reconciler.Reconcile(testAssets.Ctx, "foo/stock-pvc-pr"); err != nil {
		if ok, _ := controller.IsRequeueKey(err); !ok {
			t.Fatalf("unexpected Reconcile error: %v", err)
		}
	}

	pvcs, err := testAssets.Clients.Kube.CoreV1().PersistentVolumeClaims("foo").List(testAssets.Ctx, metav1.ListOptions{})
	if err != nil || len(pvcs.Items) != 1 {
		t.Fatalf("expected 1 PVC for stock PipelineRun, got %d (err=%v)", len(pvcs.Items), err)
	}
	sts, err := testAssets.Clients.Kube.AppsV1().StatefulSets("foo").List(testAssets.Ctx, metav1.ListOptions{})
	if err != nil || len(sts.Items) != 1 {
		t.Fatalf("expected 1 AffinityAssistant StatefulSet for stock PipelineRun, got %d (err=%v)", len(sts.Items), err)
	}
}

func TestActorPipelineRun_FailurePreservesSuspendedWorkspaceActor(t *testing.T) {
	failTask := &v1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "broken-task", Namespace: "foo"},
		Spec: v1.TaskSpec{
			Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
			Steps: []v1.Step{{
				Name:   "fail",
				Image:  "alpine:3.20",
				Script: "echo partial-work > $(workspaces.source.path)/state.txt && exit 3",
			}},
		},
	}
	p := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "fail-pipeline", Namespace: "foo"},
		Spec: v1.PipelineSpec{
			Workspaces: []v1.PipelineWorkspaceDeclaration{{Name: "shared"}},
			Tasks: []v1.PipelineTask{{
				Name:       "broken",
				TaskRef:    &v1.TaskRef{Name: "broken-task"},
				Workspaces: []v1.WorkspacePipelineTaskBinding{{Name: "source", Workspace: "shared"}},
			}},
		},
	}
	pr := &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "fail-release", Namespace: "foo", UID: "uid-fail-pr"},
		Spec: v1.PipelineRunSpec{
			PipelineRef: &v1.PipelineRef{Name: "fail-pipeline"},
			Workspaces: []v1.WorkspaceBinding{{
				Name:  "shared",
				Actor: &v1.ActorWorkspaceSource{Atespace: "ci"},
			}},
		},
	}
	d := test.Data{
		Pipelines:    []*v1.Pipeline{p},
		PipelineRuns: []*v1.PipelineRun{pr},
		Tasks:        []*v1.Task{failTask},
		ServiceAccounts: []*corev1.ServiceAccount{{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
		}},
	}

	rt := fakecontrol.NewTestRuntime(t, actorPRTestSupImg, map[string]string{
		"alpine:3.20": actorPRTestAlpineImg,
	})
	baseCtx, clients, informers, prCtl, trCtl, cancel := setupPipelineAndTaskRunControllers(t, d)
	defer cancel()
	ctx := ateactor.WithRuntime(baseCtx, rt)

	for round := 0; round < 8; round++ {
		_ = prCtl.Reconciler.Reconcile(ctx, "foo/fail-release")
		syncPipelineAndTaskInformers(t, ctx, "foo", clients, informers)
		trs, _ := clients.Pipeline.TektonV1().TaskRuns("foo").List(ctx, metav1.ListOptions{})
		for _, childTR := range trs.Items {
			if !childTR.IsDone() {
				_ = trCtl.Reconciler.Reconcile(ctx, "foo/"+childTR.Name)
				syncPipelineAndTaskInformers(t, ctx, "foo", clients, informers)
			}
		}
		cur, _ := clients.Pipeline.TektonV1().PipelineRuns("foo").Get(ctx, "fail-release", metav1.GetOptions{})
		if cur.IsDone() {
			pr = cur
			break
		}
	}

	cond := pr.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil || !cond.IsFalse() {
		t.Fatalf("expected PipelineRun Succeeded=False, got %+v", cond)
	}
	if len(rt.Server.DeletedActors()) != 0 {
		t.Fatalf("expected failed PipelineRun NOT to delete trunk Workspace Actor, deleted=%v", rt.Server.DeletedActors())
	}
	actor, err := rt.Control().GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-fail-release"},
	})
	if err != nil {
		t.Fatalf("expected trunk Workspace Actor ci/ws-fail-release to remain available: %v", err)
	}
	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("expected trunk Workspace Actor SUSPENDED, got %v", actor.GetStatus().GetState())
	}
}

func TestActorCRD_OpenAPISchemasIncludeActorFields(t *testing.T) {
	crdFiles := []string{
		"300-pipelinerun.yaml",
		"300-taskrun.yaml",
		"300-customrun.yaml",
	}
	wantProps := []string{"atespace", "sourceTag", "skipTasks", "workerSelector", "egressSecret", "egressHosts"}

	for _, file := range crdFiles {
		path := filepath.Join("..", "..", "..", "config", "300-crds", file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(raw, &crd); err != nil {
			t.Fatalf("unmarshal %s: %v", file, err)
		}
		if len(crd.Spec.Versions) == 0 {
			t.Fatalf("%s has no versions", file)
		}
		for _, ver := range crd.Spec.Versions {
			specProps := ver.Schema.OpenAPIV3Schema.Properties["spec"].Properties
			wsProp, ok := specProps["workspaces"]
			if !ok || wsProp.Items == nil || wsProp.Items.Schema == nil {
				t.Fatalf("%s version %s missing spec.workspaces.items schema", file, ver.Name)
			}
			actorSchema, ok := wsProp.Items.Schema.Properties["actor"]
			if !ok {
				t.Fatalf("%s version %s missing spec.workspaces[].actor OpenAPI property", file, ver.Name)
			}
			for _, prop := range wantProps {
				if _, exists := actorSchema.Properties[prop]; !exists {
					t.Fatalf("%s version %s actor OpenAPI schema missing property %q", file, ver.Name, prop)
				}
			}
		}
	}
}
