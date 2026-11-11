package taskrun

import (
	"strings"
	"testing"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor/fakecontrol"
	"github.com/tektoncd/pipeline/test"
	"go.ate.dev/substrate/api/ateapi/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
)

const (
	actorTestSupImg    = "gcr.io/tekton-actors/step-supervisor@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	actorTestGolangImg = "golang@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	actorTestAlpineImg = "alpine@sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func TestActorTaskRun_MultiStepResultsEgressAndSuspend(t *testing.T) {
	task := &v1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "build-and-sbom", Namespace: "foo"},
		Spec: v1.TaskSpec{
			Workspaces: []v1.WorkspaceDeclaration{
				{Name: "source"},
				{Name: "cfg"},
			},
			Results: []v1.TaskResult{
				{Name: "commit"},
				{Name: "sbom"},
			},
			Steps: []v1.Step{
				{
					Name:  "compile",
					Image: "golang:1.23",
					Script: `#!/bin/sh
set -eu
cat $(workspaces.cfg.path)/banner.txt > $(workspaces.source.path)/artifact.txt
printf "deadbeef1234" > $(results.commit.path)
`,
				},
				{
					Name:  "generate-sbom",
					Image: "alpine:3.20",
					Script: `#!/bin/sh
set -eu
grep -q "hello-from-configmap" $(workspaces.source.path)/artifact.txt
head -c 6144 /dev/zero | tr '\0' 'S' > $(results.sbom.path)
`,
				},
			},
		},
	}

	tr := &v1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tr-actor-multistep",
			Namespace: "foo",
			UID:       "uid-tr-actor-1",
			Annotations: map[string]string{
				ateactor.AnnotationActorMode:            ateactor.ActorModeTrunk,
				ateactor.AnnotationActorName:            "ws-release-1",
				ateactor.AnnotationRetainWorkspaceActor: "true",
			},
		},
		Spec: v1.TaskRunSpec{
			TaskRef: &v1.TaskRef{Name: "build-and-sbom"},
			Workspaces: []v1.WorkspaceBinding{
				{
					Name: "source",
					Actor: &v1.ActorWorkspaceSource{
						Atespace:     "ci",
						EgressSecret: "git-creds/token",
					},
				},
				{
					Name: "cfg",
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "build-cfg"},
					},
				},
			},
		},
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "build-cfg", Namespace: "foo"},
		Data:       map[string]string{"banner.txt": "hello-from-configmap\n"},
	}

	d := test.Data{
		Tasks:      []*v1.Task{task},
		TaskRuns:   []*v1.TaskRun{tr},
		ConfigMaps: []*corev1.ConfigMap{cm},
		ServiceAccounts: []*corev1.ServiceAccount{{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
		}},
	}

	rt := fakecontrol.NewTestRuntime(t, actorTestSupImg, map[string]string{
		"golang:1.23": actorTestGolangImg,
		"alpine:3.20": actorTestAlpineImg,
	})

	testAssets, cancel := getTaskRunController(t, d)
	defer cancel()
	ctx := ateactor.WithRuntime(testAssets.Ctx, rt)

	for attempt := 0; attempt < 6; attempt++ {
		err := testAssets.Controller.Reconciler.Reconcile(ctx, "foo/tr-actor-multistep")
		if err != nil {
			if ok, _ := controller.IsRequeueKey(err); !ok {
				t.Fatalf("Reconcile attempt %d unexpected error: %v", attempt, err)
			}
		}
		updated, getErr := testAssets.Clients.Pipeline.TektonV1().TaskRuns("foo").Get(ctx, "tr-actor-multistep", metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("get TaskRun: %v", getErr)
		}
		// Keep informer cache synced with API server writes in unit test loop.
		_ = testAssets.Informers.TaskRun.Informer().GetIndexer().Update(updated)
		if updated.IsDone() {
			tr = updated
			break
		}
	}

	cond := tr.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil || !cond.IsTrue() {
		t.Fatalf("expected TaskRun Succeeded=True, got %+v", cond)
	}
	if tr.Status.StartTime == nil || tr.Status.CompletionTime == nil {
		t.Fatalf("expected StartTime and CompletionTime to be populated: %+v", tr.Status)
	}
	if len(tr.Status.Steps) != 2 {
		t.Fatalf("expected 2 step statuses, got %d", len(tr.Status.Steps))
	}
	for i, st := range tr.Status.Steps {
		if st.Terminated == nil || st.Terminated.ExitCode != 0 {
			t.Fatalf("step %d not cleanly terminated: %+v", i, st)
		}
	}
	if tr.Status.Steps[0].Container != "c0" || tr.Status.Steps[1].Container != "c1" {
		t.Fatalf("expected containers c0 and c1, got %q and %q", tr.Status.Steps[0].Container, tr.Status.Steps[1].Container)
	}

	recorded := rt.Server.RecordedSteps("ci", "ws-release-1")
	if len(recorded) != 2 || recorded[0].TargetAddr != "" || recorded[1].TargetAddr != "127.0.0.1:8091" {
		t.Fatalf("unexpected recorded step routing: %+v", recorded)
	}

	resultsByName := map[string]string{}
	for _, r := range tr.Status.Results {
		resultsByName[r.Name] = r.Value.StringVal
	}
	if resultsByName["commit"] != "deadbeef1234" {
		t.Fatalf("commit result = %q, want deadbeef1234", resultsByName["commit"])
	}
	if len(resultsByName["sbom"]) != 6144 || resultsByName["sbom"] != strings.Repeat("S", 6144) {
		t.Fatalf("expected 6144-byte (>4 KiB) sbom result, got len=%d", len(resultsByName["sbom"]))
	}

	// Verify ZERO Kubernetes Pods were created.
	pods, err := testAssets.Clients.Kube.CoreV1().Pods("foo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("expected 0 Kubernetes Pods for Actor TaskRun, got %d", len(pods.Items))
	}

	// Verify EgressPolicy with ate-secret:// was configured.
	pol, err := rt.Control().GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{
		Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-release-1"},
		Name:  "default",
	})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	gotCredURI := pol.GetRules()[0].GetHttp().GetEffects().GetReplaceHeaders()[0].GetCredentialUri()
	if gotCredURI != "ate-secret://k8s.io/default/foo/git-creds/token" {
		t.Fatalf("CredentialUri = %q", gotCredURI)
	}

	// Verify trunk Actor was synchronously suspended at TaskRun completion.
	actor, err := rt.Control().GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-release-1"},
	})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("expected Actor SUSPENDED after TaskRun completion, got %v", actor.GetStatus().GetState())
	}
	if tr.Annotations[ateactor.AnnotationWorkerPod] != "ate-worker-0" {
		t.Fatalf("expected worker-pod annotation ate-worker-0, got %q", tr.Annotations[ateactor.AnnotationWorkerPod])
	}
}

func TestActorTaskRun_StandaloneCleanupAndStepFailure(t *testing.T) {
	t.Run("standalone deletes actor on success when not retained", func(t *testing.T) {
		tr := &v1.TaskRun{
			ObjectMeta: metav1.ObjectMeta{Name: "standalone-ok", Namespace: "foo", UID: "uid-sa-1"},
			Spec: v1.TaskRunSpec{
				Workspaces: []v1.WorkspaceBinding{{
					Name:  "source",
					Actor: &v1.ActorWorkspaceSource{Atespace: "ci"},
				}},
				TaskSpec: &v1.TaskSpec{
					Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
					Steps: []v1.Step{{
						Name:   "check",
						Image:  "alpine:3.20",
						Script: "echo ok > $(workspaces.source.path)/ok.txt",
					}},
				},
			},
		}
		d := test.Data{
			TaskRuns: []*v1.TaskRun{tr},
			ServiceAccounts: []*corev1.ServiceAccount{{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
			}},
		}
		rt := fakecontrol.NewTestRuntime(t, actorTestSupImg, nil)
		testAssets, cancel := getTaskRunController(t, d)
		defer cancel()
		ctx := ateactor.WithRuntime(testAssets.Ctx, rt)

		for i := 0; i < 4; i++ {
			_ = testAssets.Controller.Reconciler.Reconcile(ctx, "foo/standalone-ok")
			updated, _ := testAssets.Clients.Pipeline.TektonV1().TaskRuns("foo").Get(ctx, "standalone-ok", metav1.GetOptions{})
			_ = testAssets.Informers.TaskRun.Informer().GetIndexer().Update(updated)
			if updated.IsDone() {
				tr = updated
				break
			}
		}
		if !tr.IsSuccessful() {
			t.Fatalf("expected standalone TaskRun to succeed: %+v", tr.Status.Conditions)
		}
		deleted := rt.Server.DeletedActors()
		if len(deleted) != 1 || deleted[0] != "ci/tr-standalone-ok" {
			t.Fatalf("expected standalone Actor ci/tr-standalone-ok to be deleted, got %v", deleted)
		}
	})

	t.Run("step failure suspends actor and marks TaskRun Failed", func(t *testing.T) {
		tr := &v1.TaskRun{
			ObjectMeta: metav1.ObjectMeta{Name: "actor-fail", Namespace: "foo", UID: "uid-fail-1"},
			Spec: v1.TaskRunSpec{
				Workspaces: []v1.WorkspaceBinding{{
					Name:  "source",
					Actor: &v1.ActorWorkspaceSource{Atespace: "ci"},
				}},
				TaskSpec: &v1.TaskSpec{
					Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
					Steps: []v1.Step{{
						Name:   "boom",
						Image:  "alpine:3.20",
						Script: "exit 7",
					}},
				},
			},
		}
		d := test.Data{
			TaskRuns: []*v1.TaskRun{tr},
			ServiceAccounts: []*corev1.ServiceAccount{{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
			}},
		}
		rt := fakecontrol.NewTestRuntime(t, actorTestSupImg, nil)
		testAssets, cancel := getTaskRunController(t, d)
		defer cancel()
		ctx := ateactor.WithRuntime(testAssets.Ctx, rt)

		for i := 0; i < 4; i++ {
			_ = testAssets.Controller.Reconciler.Reconcile(ctx, "foo/actor-fail")
			updated, _ := testAssets.Clients.Pipeline.TektonV1().TaskRuns("foo").Get(ctx, "actor-fail", metav1.GetOptions{})
			_ = testAssets.Informers.TaskRun.Informer().GetIndexer().Update(updated)
			if updated.IsDone() {
				tr = updated
				break
			}
		}
		cond := tr.Status.GetCondition(apis.ConditionSucceeded)
		if cond == nil || !cond.IsFalse() {
			t.Fatalf("expected TaskRun Failed, got %+v", cond)
		}
		if len(tr.Status.Steps) != 1 || tr.Status.Steps[0].Terminated == nil || tr.Status.Steps[0].Terminated.ExitCode != 7 {
			t.Fatalf("expected Terminated step exitCode=7, got %+v", tr.Status.Steps)
		}
		actor, err := rt.Control().GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "tr-actor-fail"},
		})
		if err != nil {
			t.Fatalf("expected failed Actor to remain suspended for rescue, got err: %v", err)
		}
		if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
			t.Fatalf("expected failed Actor SUSPENDED, got %v", actor.GetStatus().GetState())
		}
	})
}

func TestStockTaskRun_WithoutActorWorkspaceCreatesPod(t *testing.T) {
	tr := &v1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{Name: "stock-pod-tr", Namespace: "foo", UID: "uid-stock-1"},
		Spec: v1.TaskRunSpec{
			Workspaces: []v1.WorkspaceBinding{{
				Name:     "source",
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			}},
			TaskSpec: &v1.TaskSpec{
				Workspaces: []v1.WorkspaceDeclaration{{Name: "source"}},
				Steps: []v1.Step{{
					Name:    "echo",
					Image:   "alpine:3.20",
					Command: []string{"/bin/echo", "hi"},
				}},
			},
		},
	}
	d := test.Data{
		TaskRuns: []*v1.TaskRun{tr},
		ServiceAccounts: []*corev1.ServiceAccount{{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "foo"},
		}},
	}
	rt := fakecontrol.NewTestRuntime(t, actorTestSupImg, nil)
	testAssets, cancel := getTaskRunController(t, d)
	defer cancel()
	ctx := ateactor.WithRuntime(testAssets.Ctx, rt)

	err := testAssets.Controller.Reconciler.Reconcile(ctx, "foo/stock-pod-tr")
	if err != nil {
		if ok, _ := controller.IsRequeueKey(err); !ok {
			t.Fatalf("unexpected Reconcile error: %v", err)
		}
	}
	pods, err := testAssets.Clients.Kube.CoreV1().Pods("foo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected 1 Kubernetes Pod for stock TaskRun, got %d", len(pods.Items))
	}
}
