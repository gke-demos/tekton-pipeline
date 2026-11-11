// Package fakecontrol provides a high-fidelity in-memory Agent Substrate
// ControlServer and loopback Step Supervisor harness for Tekton reconciler tests.
package fakecontrol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements ateapi.ControlServer in memory with real filesystem-backed
// durable_dir volumes and loopback HTTP Step Supervisors per Actor.
type Server struct {
	ateapi.UnimplementedControlServer

	rootDir string

	mu            sync.Mutex
	atespaces     map[string]*ateapi.Atespace
	templates     map[string]*ateapi.ActorTemplate // key: atespace/name
	actors        map[string]*actorEntry           // key: atespace/name
	egressPols    map[string]*ateapi.EgressPolicy  // key: atespace/actorName
	tags          map[string]*tagEntry             // key: atespace/tagName
	updateCounts  map[string]int                   // key: atespace/actorName
	suspendCounts map[string]int                   // key: atespace/actorName
	deletedActors []string                         // elements: atespace/actorName
}

type actorEntry struct {
	proto       *ateapi.Actor
	workspaceFS string
	tektonFS    string
	snapshotFS  string
	supervisor  *loopbackSupervisor
	stepHistory []ateactor.RunStepRequest
}

type tagEntry struct {
	proto      *ateapi.Tag
	snapshotFS string
}

// NewServer constructs a FakeControlServer backed by t.TempDir().
func NewServer(tb testing.TB) *Server {
	tb.Helper()
	s := &Server{
		rootDir:       tb.TempDir(),
		atespaces:     make(map[string]*ateapi.Atespace),
		templates:     make(map[string]*ateapi.ActorTemplate),
		actors:        make(map[string]*actorEntry),
		egressPols:    make(map[string]*ateapi.EgressPolicy),
		tags:          make(map[string]*tagEntry),
		updateCounts:  make(map[string]int),
		suspendCounts: make(map[string]int),
	}
	tb.Cleanup(s.Shutdown)
	return s
}

// Shutdown closes any running loopback Step Supervisor servers.
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.actors {
		if entry.supervisor != nil {
			entry.supervisor.Close()
			entry.supervisor = nil
		}
	}
}

// ActorUpdateCount returns how many UpdateActor calls succeeded for atespace/name.
func (s *Server) ActorUpdateCount(atespace, name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateCounts[atespace+"/"+name]
}

// ActorSuspendCount returns how many SuspendActor calls succeeded for atespace/name.
func (s *Server) ActorSuspendCount(atespace, name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suspendCounts[atespace+"/"+name]
}

// DeletedActors returns all "atespace/name" identifiers deleted via DeleteActor.
func (s *Server) DeletedActors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletedActors...)
}

// RecordedSteps returns the RunStepRequests dispatched to atespace/actorName.
func (s *Server) RecordedSteps(atespace, actorName string) []ateactor.RunStepRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.actors[atespace+"/"+actorName]
	if entry == nil {
		return nil
	}
	return append([]ateactor.RunStepRequest(nil), entry.stepHistory...)
}

func (s *Server) supervisorURL(atespace, actorName string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.actors[atespace+"/"+actorName]
	if entry == nil {
		return "", fmt.Errorf("actor %s/%s not found", atespace, actorName)
	}
	if entry.proto.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING || entry.supervisor == nil {
		return "", fmt.Errorf("actor %s/%s is not RUNNING (state=%s)", atespace, actorName, entry.proto.GetStatus().GetState())
	}
	return entry.supervisor.srv.URL, nil
}

// TestRuntime implements ateactor.Runtime over an in-memory bufconn gRPC
// channel to FakeControlServer and loopback HTTP Step Supervisors.
type TestRuntime struct {
	Server          *Server
	conn            *grpc.ClientConn
	lis             *bufconn.Listener
	ctrl            ateapi.ControlClient
	supervisorImage string
	digests         map[string]string
}

// NewTestRuntime starts an in-memory gRPC ControlServer and returns an
// ateactor.Runtime suitable for TaskRun and PipelineRun reconciler tests.
func NewTestRuntime(tb testing.TB, supervisorImage string, tagToDigest map[string]string) *TestRuntime {
	tb.Helper()
	srv := NewServer(tb)
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	ateapi.RegisterControlServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		tb.Fatalf("dial bufconn: %v", err)
	}

	digests := make(map[string]string, len(tagToDigest))
	for k, v := range tagToDigest {
		digests[k] = v
	}
	if supervisorImage == "" {
		supervisorImage = "gcr.io/tekton-actors/step-supervisor@sha256:0000000000000000000000000000000000000000000000000000000000000001"
	}

	rt := &TestRuntime{
		Server:          srv,
		conn:            conn,
		lis:             lis,
		ctrl:            ateapi.NewControlClient(conn),
		supervisorImage: supervisorImage,
		digests:         digests,
	}
	tb.Cleanup(func() {
		_ = rt.Close()
		grpcSrv.Stop()
	})
	return rt
}

func (r *TestRuntime) Control() ateapi.ControlClient {
	return r.ctrl
}

func (r *TestRuntime) SupervisorImage() string {
	return r.supervisorImage
}

func (r *TestRuntime) ResolveImageDigest(_ context.Context, ref string) (string, error) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return "", fmt.Errorf("empty image reference")
	}
	if ateactor.HasPinnedDigest(trimmed) {
		return trimmed, nil
	}
	if pinned, ok := r.digests[trimmed]; ok {
		return pinned, nil
	}
	// Deterministic synthetic digest for unmapped tags in unit tests unless "@invalid" is passed.
	if strings.Contains(trimmed, "invalid") {
		return "", fmt.Errorf("invalid image %q", trimmed)
	}
	repo := trimmed
	if idx := strings.LastIndex(repo, ":"); idx > strings.LastIndex(repo, "/") {
		repo = repo[:idx]
	}
	sum := fmt.Sprintf("%064x", crcHash(trimmed))
	return repo + "@sha256:" + sum, nil
}

func crcHash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func (r *TestRuntime) SupervisorClient(atespace, actorName string) *ateactor.SupervisorClient {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			rawURL, err := r.Server.supervisorURL(atespace, actorName)
			if err != nil {
				return nil, err
			}
			u, err := url.Parse(rawURL)
			if err != nil {
				return nil, err
			}
			var d net.Dialer
			return d.DialContext(ctx, "tcp", u.Host)
		},
	}
	return ateactor.NewSupervisorClient(fmt.Sprintf("http://%s:8080", actorName), &http.Client{Transport: transport})
}

func (r *TestRuntime) Close() error {
	var err error
	if r.conn != nil {
		err = r.conn.Close()
	}
	if r.lis != nil {
		_ = r.lis.Close()
	}
	return err
}

// --- ControlServer RPCs ---

func (s *Server) GetAtespace(_ context.Context, req *ateapi.GetAtespaceRequest) (*ateapi.Atespace, error) {
	if req.GetAtespace().GetAtespace() != "" {
		return nil, status.Error(codes.InvalidArgument, "Atespace is global-scoped; metadata.atespace must be empty")
	}
	name := req.GetAtespace().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.atespaces[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "atespace %q not found", name)
	}
	return proto.CloneOf(as), nil
}

func (s *Server) CreateAtespace(_ context.Context, req *ateapi.CreateAtespaceRequest) (*ateapi.Atespace, error) {
	in := req.GetAtespace()
	if in == nil || in.GetMetadata().GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace metadata.name is required")
	}
	if in.GetMetadata().GetAtespace() != "" {
		return nil, status.Error(codes.InvalidArgument, "atespace metadata.atespace must be empty")
	}
	name := in.GetMetadata().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.atespaces[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "atespace %q already exists", name)
	}
	now := timestamppb.Now()
	stored := &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{
			Name:       name,
			Uid:        uuid.NewString(),
			Version:    1,
			CreateTime: now,
			UpdateTime: now,
		},
	}
	s.atespaces[name] = stored
	return proto.CloneOf(stored), nil
}

func (s *Server) GetActorTemplate(_ context.Context, req *ateapi.GetActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	key := req.GetActorTemplate().GetAtespace() + "/" + req.GetActorTemplate().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor template %q not found", key)
	}
	return proto.CloneOf(tmpl), nil
}

func (s *Server) CreateActorTemplate(_ context.Context, req *ateapi.CreateActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	in := req.GetActorTemplate()
	if in == nil || in.GetMetadata().GetAtespace() == "" || in.GetMetadata().GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_template metadata.atespace and name are required")
	}
	if err := validateTemplateSpec(in); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.atespaces[in.GetMetadata().GetAtespace()]; !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "atespace %q not found", in.GetMetadata().GetAtespace())
	}
	key := in.GetMetadata().GetAtespace() + "/" + in.GetMetadata().GetName()
	if _, exists := s.templates[key]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "actor template %q already exists", key)
	}
	now := timestamppb.Now()
	stored := proto.CloneOf(in)
	stored.Metadata.Uid = uuid.NewString()
	stored.Metadata.Version = 1
	stored.Metadata.CreateTime = now
	stored.Metadata.UpdateTime = now
	s.templates[key] = stored
	return proto.CloneOf(stored), nil
}

func (s *Server) ListActorTemplates(_ context.Context, req *ateapi.ListActorTemplatesRequest) (*ateapi.ListActorTemplatesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*ateapi.ActorTemplate
	for _, t := range s.templates {
		if req.GetAtespace() == "" || t.GetMetadata().GetAtespace() == req.GetAtespace() {
			out = append(out, proto.CloneOf(t))
		}
	}
	return &ateapi.ListActorTemplatesResponse{ActorTemplates: out}, nil
}

func validateTemplateSpec(tmpl *ateapi.ActorTemplate) error {
	if len(tmpl.GetContainers()) == 0 {
		return errors.New("at least one container is required")
	}
	if tmpl.GetResources().GetLimits().GetCpuMillicores() <= 0 || tmpl.GetResources().GetLimits().GetMemoryBytes() <= 0 {
		return errors.New("both cpu_millicores and memory_bytes limits must be > 0")
	}
	vols := make(map[string]bool, len(tmpl.GetVolumes()))
	for _, v := range tmpl.GetVolumes() {
		vols[v.GetName()] = true
		if v.GetImage() != nil && !ateactor.HasPinnedDigest(v.GetImage().GetReference()) {
			return fmt.Errorf("image volume %q reference %q must carry @sha256 digest", v.GetName(), v.GetImage().GetReference())
		}
	}
	for _, c := range tmpl.GetContainers() {
		if !ateactor.HasPinnedDigest(c.GetImage()) {
			return fmt.Errorf("container %q image %q must carry @sha256 digest", c.GetName(), c.GetImage())
		}
		var paths []string
		for _, m := range c.GetVolumeMounts() {
			if !vols[m.GetName()] {
				return fmt.Errorf("container %q mounts undeclared volume %q", c.GetName(), m.GetName())
			}
			for _, prev := range paths {
				if m.GetMountPath() == prev || strings.HasPrefix(m.GetMountPath(), prev+"/") || strings.HasPrefix(prev, m.GetMountPath()+"/") {
					return fmt.Errorf("container %q has nested volume mounts %q and %q", c.GetName(), prev, m.GetMountPath())
				}
			}
			paths = append(paths, m.GetMountPath())
		}
	}
	return nil
}

func (s *Server) GetActor(_ context.Context, req *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.actors[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	return proto.CloneOf(entry.proto), nil
}

func (s *Server) ListActors(_ context.Context, req *ateapi.ListActorsRequest) (*ateapi.ListActorsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*ateapi.Actor
	for _, e := range s.actors {
		if req.GetAtespace() == "" || e.proto.GetMetadata().GetAtespace() == req.GetAtespace() {
			out = append(out, proto.CloneOf(e.proto))
		}
	}
	return &ateapi.ListActorsResponse{Actors: out}, nil
}

func (s *Server) CreateActor(_ context.Context, req *ateapi.CreateActorRequest) (*ateapi.Actor, error) {
	in := req.GetActor()
	if in == nil || in.GetMetadata().GetAtespace() == "" || in.GetMetadata().GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor metadata.atespace and name are required")
	}
	atespace := in.GetMetadata().GetAtespace()
	name := in.GetMetadata().GetName()
	key := atespace + "/" + name

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.atespaces[atespace]; !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "atespace %q not found", atespace)
	}
	if _, exists := s.actors[key]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "actor %q already exists", key)
	}
	tmplKey := in.GetActorTemplate().GetAtespace() + "/" + in.GetActorTemplate().GetName()
	tmpl, ok := s.templates[tmplKey]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "actor template %q not found", tmplKey)
	}

	actorRoot := filepath.Join(s.rootDir, "actors", atespace, name+"-"+uuid.NewString()[:8])
	wsDir := filepath.Join(actorRoot, "workspace")
	tknDir := filepath.Join(actorRoot, "tekton")
	snapDir := filepath.Join(actorRoot, "snapshot")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir ws: %v", err)
	}
	if err := os.MkdirAll(tknDir, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir tekton: %v", err)
	}

	var snapshots []*ateapi.Snapshot
	var lastGen int32
	if in.GetSourceTag() != nil {
		tagKey := in.GetSourceTag().GetAtespace() + "/" + in.GetSourceTag().GetName()
		tEntry, tagOK := s.tags[tagKey]
		if !tagOK {
			return nil, status.Errorf(codes.FailedPrecondition, "source tag %q not found", tagKey)
		}
		if tEntry.proto.GetStatus().GetActorTemplateUid() != tmpl.GetMetadata().GetUid() {
			return nil, status.Errorf(codes.FailedPrecondition, "source Tag must be taken from an actor with ActorTemplate uid %q (got %q)", tEntry.proto.GetStatus().GetActorTemplateUid(), tmpl.GetMetadata().GetUid())
		}
		if err := copyDirContents(filepath.Join(tEntry.snapshotFS, "workspace"), wsDir); err != nil {
			return nil, status.Errorf(codes.Internal, "seed workspace from tag: %v", err)
		}
		if err := copyDirContents(filepath.Join(tEntry.snapshotFS, "tekton"), tknDir); err != nil {
			return nil, status.Errorf(codes.Internal, "seed tekton from tag: %v", err)
		}
		if err := copyDirContents(tEntry.snapshotFS, snapDir); err != nil {
			return nil, status.Errorf(codes.Internal, "seed snapshot from tag: %v", err)
		}
		snap := proto.CloneOf(tEntry.proto.GetStatus().GetSnapshot())
		snap.Generation = 1
		snap.Owner = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
		snapshots = []*ateapi.Snapshot{snap}
		lastGen = 1
	}

	now := timestamppb.Now()
	stored := proto.CloneOf(in)
	stored.Metadata = &ateapi.ResourceMetadata{
		Atespace:   atespace,
		Name:       name,
		Uid:        uuid.NewString(),
		Version:    1,
		CreateTime: now,
		UpdateTime: now,
	}
	stored.Status = &ateapi.ActorStatus{
		State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		Snapshots:              snapshots,
		LastAssignedGeneration: lastGen,
	}
	s.actors[key] = &actorEntry{
		proto:       stored,
		workspaceFS: wsDir,
		tektonFS:    tknDir,
		snapshotFS:  snapDir,
	}
	return proto.CloneOf(stored), nil
}

func (s *Server) UpdateActor(_ context.Context, req *ateapi.UpdateActorRequest) (*ateapi.Actor, error) {
	in := req.GetActor()
	if in == nil || in.GetMetadata().GetAtespace() == "" || in.GetMetadata().GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor metadata.atespace and name are required")
	}
	if in.GetMetadata().GetUid() == "" || in.GetMetadata().GetVersion() == 0 {
		return nil, status.Error(codes.InvalidArgument, "actor metadata.uid and version preconditions are required")
	}
	key := in.GetMetadata().GetAtespace() + "/" + in.GetMetadata().GetName()

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.actors[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	cur := entry.proto
	if cur.GetMetadata().GetUid() != in.GetMetadata().GetUid() || cur.GetMetadata().GetVersion() != in.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "concurrent update conflict")
	}
	if !proto.Equal(cur.GetSourceTag(), in.GetSourceTag()) {
		return nil, status.Error(codes.InvalidArgument, "source_tag is immutable")
	}

	if !proto.Equal(cur.GetActorTemplate(), in.GetActorTemplate()) {
		if cur.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
			return nil, status.Errorf(codes.FailedPrecondition, "actor must be ACTOR_STATE_SUSPENDED to change its actor template (got %s)", cur.GetStatus().GetState())
		}
		oldTmpl := s.templates[cur.GetActorTemplate().GetAtespace()+"/"+cur.GetActorTemplate().GetName()]
		newTmpl, exists := s.templates[in.GetActorTemplate().GetAtespace()+"/"+in.GetActorTemplate().GetName()]
		if !exists {
			return nil, status.Errorf(codes.FailedPrecondition, "new actor template %s not found", in.GetActorTemplate().GetName())
		}
		if oldTmpl != nil {
			if err := validateTemplateVolumesUnchanged(oldTmpl, newTmpl); err != nil {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
		}
	}

	updated := proto.CloneOf(in)
	updated.Metadata = proto.CloneOf(cur.GetMetadata())
	updated.Metadata.Version++
	updated.Metadata.UpdateTime = timestamppb.Now()
	updated.Status = proto.CloneOf(cur.GetStatus())
	entry.proto = updated
	s.updateCounts[key]++
	return proto.CloneOf(updated), nil
}

func validateTemplateVolumesUnchanged(oldTmpl, newTmpl *ateapi.ActorTemplate) error {
	if !slices.EqualFunc(oldTmpl.GetVolumes(), newTmpl.GetVolumes(), func(a, b *ateapi.Volume) bool {
		return proto.Equal(a, b)
	}) {
		return errors.New("volumes differ between the current and the new actor template; volumes must be identical to repoint an actor")
	}
	newContainers := make(map[string]*ateapi.Container, len(newTmpl.GetContainers()))
	for _, c := range newTmpl.GetContainers() {
		newContainers[c.GetName()] = c
	}
	for _, oldC := range oldTmpl.GetContainers() {
		newC, ok := newContainers[oldC.GetName()]
		if !ok {
			continue
		}
		if !slices.EqualFunc(oldC.GetVolumeMounts(), newC.GetVolumeMounts(), func(a, b *ateapi.VolumeMount) bool {
			return proto.Equal(a, b)
		}) {
			return fmt.Errorf("volume mounts of container %q differ between the current and the new actor template", oldC.GetName())
		}
	}
	return nil
}

func (s *Server) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.actors[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	if entry.proto.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
		return &ateapi.ResumeActorResponse{Actor: proto.CloneOf(entry.proto), Resumed: false}, nil
	}

	if entry.supervisor != nil {
		entry.supervisor.Close()
	}
	entry.supervisor = newLoopbackSupervisor(entry.workspaceFS, entry.tektonFS, func(stepReq ateactor.RunStepRequest) {
		s.mu.Lock()
		entry.stepHistory = append(entry.stepHistory, stepReq)
		s.mu.Unlock()
	})
	entry.proto.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	entry.proto.Status.AssignedNode = "worker-node-0"
	entry.proto.Status.WorkerAssignment = &ateapi.WorkerAssignment{
		Worker:          &ateapi.ObjectRef{Name: "worker-0"},
		WorkerNamespace: "ate-system",
		WorkerPool:      "default-pool",
		WorkerPod:       "ate-worker-0",
		WorkerPodUid:    "00000000-0000-0000-0000-000000000001",
		WorkerPodIps:    []string{"10.0.0.10"},
		NodeName:        "worker-node-0",
	}
	entry.proto.Metadata.Version++
	entry.proto.Metadata.UpdateTime = timestamppb.Now()
	return &ateapi.ResumeActorResponse{Actor: proto.CloneOf(entry.proto), Resumed: true}, nil
}

func (s *Server) SuspendActor(_ context.Context, req *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.actors[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	if entry.proto.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		return &ateapi.SuspendActorResponse{Actor: proto.CloneOf(entry.proto)}, nil
	}
	if entry.supervisor != nil {
		entry.supervisor.Close()
		entry.supervisor = nil
	}
	_ = os.RemoveAll(entry.snapshotFS)
	if err := copyDirContents(entry.workspaceFS, filepath.Join(entry.snapshotFS, "workspace")); err != nil {
		return nil, status.Errorf(codes.Internal, "snapshot workspace: %v", err)
	}
	if err := copyDirContents(entry.tektonFS, filepath.Join(entry.snapshotFS, "tekton")); err != nil {
		return nil, status.Errorf(codes.Internal, "snapshot tekton: %v", err)
	}

	tmplKey := entry.proto.GetActorTemplate().GetAtespace() + "/" + entry.proto.GetActorTemplate().GetName()
	var tmplUID string
	if tmpl := s.templates[tmplKey]; tmpl != nil {
		tmplUID = tmpl.GetMetadata().GetUid()
	}

	gen := entry.proto.GetStatus().GetLastAssignedGeneration() + 1
	snapState := ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
	snapUUID := "snap-" + strconv.Itoa(int(gen))
	entry.proto.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	entry.proto.Status.WorkerAssignment = nil
	entry.proto.Status.LastAssignedGeneration = gen
	entry.proto.Status.Snapshots = []*ateapi.Snapshot{{
		Uuid:             snapUUID,
		Generation:       gen,
		Owner:            ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:         ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ActorTemplateUid: tmplUID,
		DurableSnapshot: &ateapi.SnapshotStorage{
			State: snapState,
			Object: &ateapi.ObjectSnapshot{
				SnapshotUri: fmt.Sprintf("gs://fake-snapshots/atespaces/%s/actors/%s/%s", req.GetActor().GetAtespace(), entry.proto.GetMetadata().GetUid(), snapUUID),
			},
		},
	}}
	entry.proto.Metadata.Version++
	entry.proto.Metadata.UpdateTime = timestamppb.Now()
	s.suspendCounts[key]++
	return &ateapi.SuspendActorResponse{Actor: proto.CloneOf(entry.proto)}, nil
}

func (s *Server) DeleteActor(_ context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error) {
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.actors[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	if entry.supervisor != nil {
		entry.supervisor.Close()
		entry.supervisor = nil
	}
	deleted := proto.CloneOf(entry.proto)
	delete(s.actors, key)
	delete(s.egressPols, key)
	s.deletedActors = append(s.deletedActors, key)
	return deleted, nil
}

func (s *Server) CreateActorEgressPolicy(_ context.Context, req *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	actorRef := req.GetActor()
	pol := req.GetEgressPolicy()
	if actorRef == nil || pol == nil {
		return nil, status.Error(codes.InvalidArgument, "actor and egress_policy are required")
	}
	if pol.GetMetadata().GetName() != "default" {
		return nil, status.Error(codes.InvalidArgument, `egress_policy metadata.name must be "default"`)
	}
	if pol.GetMetadata().GetAtespace() != actorRef.GetAtespace() {
		return nil, status.Error(codes.InvalidArgument, "egress_policy metadata.atespace must match actor.atespace")
	}
	if err := validateEgressRules(pol.GetRules()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	key := actorRef.GetAtespace() + "/" + actorRef.GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.actors[key]; !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	if _, exists := s.egressPols[key]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "egress policy default already exists on %q", key)
	}
	stored := proto.CloneOf(pol)
	stored.Metadata.Uid = uuid.NewString()
	stored.Metadata.Version = 1
	s.egressPols[key] = stored
	return proto.CloneOf(stored), nil
}

func (s *Server) GetActorEgressPolicy(_ context.Context, req *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	pol, ok := s.egressPols[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "egress policy not found on %q", key)
	}
	return proto.CloneOf(pol), nil
}

func validateEgressRules(rules []*ateapi.EgressRule) error {
	type rulePortKey struct {
		pattern string
		port    int32 // -1 means AllPorts
	}
	seen := make(map[rulePortKey]int)
	for i, r := range rules {
		var hosts []string
		var ports *ateapi.Ports
		var defaultPort int32
		var effects *ateapi.HttpRuleEffects
		switch {
		case r.GetHttp() != nil:
			hosts = r.GetHttp().GetHostnames()
			ports = r.GetHttp().GetPorts()
			defaultPort = 80
			effects = r.GetHttp().GetEffects()
		case r.GetHttps() != nil:
			hosts = r.GetHttps().GetHostnames()
			ports = r.GetHttps().GetPorts()
			defaultPort = 443
			effects = r.GetHttps().GetEffects()
		case r.GetTlsPassthrough() != nil:
			hosts = r.GetTlsPassthrough().GetHostnames()
			ports = r.GetTlsPassthrough().GetPorts()
		default:
			return errors.New("egress rule must set http, https, or tls_passthrough")
		}
		if effects != nil {
			for _, hdr := range effects.GetReplaceHeaders() {
				if hdr.GetCredentialUri() != "" && !strings.HasPrefix(hdr.GetCredentialUri(), "ate-secret://") {
					return fmt.Errorf("invalid credential_uri %q", hdr.GetCredentialUri())
				}
			}
		}
		var pNums []int32
		if ports == nil {
			pNums = []int32{defaultPort}
		} else if ports.GetAll() != nil {
			pNums = []int32{-1}
		} else {
			pNums = ports.GetNumbers()
		}
		for _, h := range hosts {
			for _, pn := range pNums {
				k := rulePortKey{pattern: h, port: pn}
				if prevIdx, dup := seen[k]; dup && prevIdx != i {
					return fmt.Errorf("rule %d ties with rule %d on hostname %q port %d", i, prevIdx, h, pn)
				}
				seen[k] = i
			}
		}
	}
	return nil
}

func (s *Server) CreateTag(_ context.Context, req *ateapi.CreateTagRequest) (*ateapi.Tag, error) {
	in := req.GetTag()
	if in == nil || in.GetMetadata().GetAtespace() == "" || in.GetMetadata().GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "tag metadata.atespace and name are required")
	}
	atespace := in.GetMetadata().GetAtespace()
	name := in.GetMetadata().GetName()
	tagKey := atespace + "/" + name
	actorKey := in.GetSourceActor().GetAtespace() + "/" + in.GetSourceActor().GetName()

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tags[tagKey]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "tag %q already exists", tagKey)
	}
	srcActor, ok := s.actors[actorKey]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "source actor %q not found", actorKey)
	}
	if srcActor.proto.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, status.Errorf(codes.FailedPrecondition, "source actor %q must be SUSPENDED to CreateTag", actorKey)
	}
	snaps := srcActor.proto.GetStatus().GetSnapshots()
	if len(snaps) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "source actor %q has no snapshot", actorKey)
	}

	tagFS := filepath.Join(s.rootDir, "tags", atespace, name+"-"+uuid.NewString()[:8])
	if err := copyDirContents(srcActor.snapshotFS, tagFS); err != nil {
		return nil, status.Errorf(codes.Internal, "copy snapshot to tag: %v", err)
	}

	tagUID := uuid.NewString()
	snapCopy := proto.CloneOf(snaps[len(snaps)-1])
	snapCopy.Owner = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
	snapCopy.DurableSnapshot = &ateapi.SnapshotStorage{
		State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
		Object: &ateapi.ObjectSnapshot{
			SnapshotUri: fmt.Sprintf("gs://fake-snapshots/atespaces/%s/tags/%s/%s", atespace, tagUID, snapCopy.GetUuid()),
		},
	}

	now := timestamppb.Now()
	stored := &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{
			Atespace:   atespace,
			Name:       name,
			Uid:        tagUID,
			Version:    1,
			CreateTime: now,
			UpdateTime: now,
		},
		Scope:       in.GetScope(),
		SourceActor: proto.CloneOf(in.GetSourceActor()),
		Status: &ateapi.TagStatus{
			Snapshot:         snapCopy,
			ActorTemplateUid: snapCopy.GetActorTemplateUid(),
		},
	}
	s.tags[tagKey] = &tagEntry{proto: stored, snapshotFS: tagFS}
	return proto.CloneOf(stored), nil
}

func (s *Server) GetTag(_ context.Context, req *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	key := req.GetTag().GetAtespace() + "/" + req.GetTag().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.tags[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tag %q not found", key)
	}
	return proto.CloneOf(entry.proto), nil
}

func (s *Server) ListTags(_ context.Context, req *ateapi.ListTagsRequest) (*ateapi.ListTagsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*ateapi.Tag
	for _, t := range s.tags {
		if req.GetAtespace() == "" || t.proto.GetMetadata().GetAtespace() == req.GetAtespace() {
			out = append(out, proto.CloneOf(t.proto))
		}
	}
	return &ateapi.ListTagsResponse{Tags: out}, nil
}

func (s *Server) DeleteTag(_ context.Context, req *ateapi.DeleteTagRequest) (*ateapi.Tag, error) {
	key := req.GetTag().GetAtespace() + "/" + req.GetTag().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.tags[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tag %q not found", key)
	}
	delete(s.tags, key)
	return proto.CloneOf(entry.proto), nil
}

// --- Loopback Step Supervisor ---

type loopbackSupervisor struct {
	wsDir     string
	tektonDir string
	onStep    func(ateactor.RunStepRequest)
	srv       *httptest.Server

	mu    sync.Mutex
	steps map[string]*simStep
}

type simStep struct {
	mu     sync.Mutex
	status ateactor.StepStatus
	logs   bytes.Buffer
	doneCh chan struct{}
}

func newLoopbackSupervisor(wsDir, tektonDir string, onStep func(ateactor.RunStepRequest)) *loopbackSupervisor {
	ls := &loopbackSupervisor{
		wsDir:     wsDir,
		tektonDir: tektonDir,
		onStep:    onStep,
		steps:     make(map[string]*simStep),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	mux.HandleFunc("POST /v1/steps", ls.handleRunStep)
	mux.HandleFunc("GET /v1/steps/{id}", ls.handleGetStep)
	mux.HandleFunc("GET /v1/steps/{id}/logs", ls.handleStepLogs)
	mux.HandleFunc("POST /v1/files", ls.handleUploadFiles)
	mux.HandleFunc("POST /v1/rescue/patch", ls.handleRescuePatch)
	ls.srv = httptest.NewServer(mux)
	return ls
}

func (ls *loopbackSupervisor) Close() {
	if ls.srv != nil {
		ls.srv.Close()
	}
}

func (ls *loopbackSupervisor) translatePath(s string) string {
	out := strings.ReplaceAll(s, "/workspace", ls.wsDir)
	out = strings.ReplaceAll(out, "/tekton", ls.tektonDir)
	return out
}

func (ls *loopbackSupervisor) handleRunStep(w http.ResponseWriter, r *http.Request) {
	var req ateactor.RunStepRequest
	if err := jsonJSONDecode(r.Body, &req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if ls.onStep != nil {
		ls.onStep(req)
	}
	if req.StepID == "" {
		req.StepID = uuid.NewString()
	}

	ls.mu.Lock()
	if _, exists := ls.steps[req.StepID]; exists {
		ls.mu.Unlock()
		http.Error(w, `{"error":"step already exists"}`, http.StatusConflict)
		return
	}
	resultsDir := filepath.Join(ls.tektonDir, "results")
	homeDir := filepath.Join(ls.tektonDir, "home")
	stepsDir := filepath.Join(ls.tektonDir, "steps")
	for _, d := range []string{ls.wsDir, resultsDir, homeDir, stepsDir} {
		_ = os.MkdirAll(d, 0o755)
	}

	now := time.Now().UTC()
	rec := &simStep{
		status: ateactor.StepStatus{
			StepID:    req.StepID,
			Name:      req.Name,
			State:     ateactor.StepStateRunning,
			StartedAt: now,
		},
		doneCh: make(chan struct{}),
	}
	ls.steps[req.StepID] = rec
	ls.mu.Unlock()

	workDir := ls.wsDir
	if req.WorkingDir != "" {
		workDir = ls.translatePath(req.WorkingDir)
	}
	_ = os.MkdirAll(workDir, 0o755)

	var cmd *exec.Cmd
	if req.Script != "" {
		scriptText := ls.translatePath(req.Script)
		cmd = exec.Command("/bin/sh", "-e", "-c", scriptText)
	} else if len(req.Command) > 0 {
		translated := make([]string, 0, len(req.Command)+len(req.Args))
		for _, c := range req.Command {
			translated = append(translated, ls.translatePath(c))
		}
		for _, a := range req.Args {
			translated = append(translated, ls.translatePath(a))
		}
		cmd = exec.Command(translated[0], translated[1:]...)
	} else {
		http.Error(w, `{"error":"script or command required"}`, http.StatusBadRequest)
		return
	}
	cmd.Dir = workDir
	env := append([]string{}, os.Environ()...)
	env = append(env, "HOME="+homeDir)
	for _, e := range req.Env {
		env = append(env, e.Name+"="+ls.translatePath(e.Value))
	}
	cmd.Env = env

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err := cmd.Start(); err != nil {
		code := 127
		fin := time.Now().UTC()
		rec.mu.Lock()
		rec.status.State = ateactor.StepStateFailed
		rec.status.ExitCode = &code
		rec.status.FinishedAt = &fin
		rec.status.Error = err.Error()
		rec.mu.Unlock()
		close(rec.doneCh)
		writeJSONResp(w, http.StatusAccepted, rec.snapshot())
		return
	}

	resultNames := append([]string(nil), req.ResultNames...)
	go func() {
		waitErr := cmd.Wait()
		code := 0
		if waitErr != nil {
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				code = exitErr.ExitCode()
			} else {
				code = 1
			}
		}
		fin := time.Now().UTC()
		st := ateactor.StepStateCompleted
		if code != 0 {
			st = ateactor.StepStateFailed
		}
		resMap := make(map[string]string)
		for _, rn := range resultNames {
			if raw, readErr := os.ReadFile(filepath.Join(resultsDir, rn)); readErr == nil {
				resMap[rn] = string(raw)
			}
		}
		rec.mu.Lock()
		_, _ = rec.logs.Write(outBuf.Bytes())
		rec.status.LogSize = int64(rec.logs.Len())
		rec.status.ExitCode = &code
		rec.status.FinishedAt = &fin
		if len(resMap) > 0 {
			rec.status.Results = resMap
		}
		rec.status.State = st
		rec.mu.Unlock()
		close(rec.doneCh)
	}()

	writeJSONResp(w, http.StatusAccepted, rec.snapshot())
}

func (ls *loopbackSupervisor) handleGetStep(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ls.mu.Lock()
	rec, ok := ls.steps[id]
	ls.mu.Unlock()
	if !ok {
		http.Error(w, `{"error":"step not found"}`, http.StatusNotFound)
		return
	}
	if r.URL.Query().Get("wait") == "true" {
		select {
		case <-rec.doneCh:
		case <-r.Context().Done():
			return
		}
	}
	writeJSONResp(w, http.StatusOK, rec.snapshot())
}

func (ls *loopbackSupervisor) handleStepLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ls.mu.Lock()
	rec, ok := ls.steps[id]
	ls.mu.Unlock()
	if !ok {
		http.Error(w, `{"error":"step not found"}`, http.StatusNotFound)
		return
	}
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	rec.mu.Lock()
	data := rec.logs.Bytes()
	if offset < 0 || offset > int64(len(data)) {
		offset = int64(len(data))
	}
	chunk := append([]byte(nil), data[offset:]...)
	newOff := int64(len(data))
	st := rec.status.State
	rec.mu.Unlock()

	w.Header().Set("X-Log-Offset", strconv.FormatInt(newOff, 10))
	w.Header().Set("X-Step-State", string(st))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(chunk)
}

func (ls *loopbackSupervisor) handleUploadFiles(w http.ResponseWriter, r *http.Request) {
	var req ateactor.UploadFilesRequest
	if err := jsonJSONDecode(r.Body, &req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	base := ls.wsDir
	if req.SubDir != "" {
		base = filepath.Join(ls.wsDir, filepath.Clean(req.SubDir))
	}
	mode := os.FileMode(0o644)
	if req.Mode != 0 {
		mode = os.FileMode(req.Mode)
	}
	var written []string
	for rel, content := range req.Files {
		dst := filepath.Join(base, filepath.Clean(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(dst, content, mode); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		written = append(written, rel)
	}
	writeJSONResp(w, http.StatusOK, ateactor.UploadFilesResponse{WrittenFiles: written})
}

func (ls *loopbackSupervisor) handleRescuePatch(w http.ResponseWriter, r *http.Request) {
	var req ateactor.RescuePatchRequest
	if err := jsonJSONDecode(r.Body, &req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	var patched []string
	for rel, content := range req.Files {
		dst := filepath.Join(ls.wsDir, filepath.Clean(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(dst, []byte(content), 0o644); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		patched = append(patched, rel)
	}
	for _, rel := range req.DeleteFiles {
		dst := filepath.Join(ls.wsDir, filepath.Clean(rel))
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		patched = append(patched, rel)
	}
	writeJSONResp(w, http.StatusOK, ateactor.RescuePatchResponse{PatchedFiles: patched})
}

func (s *simStep) snapshot() ateactor.StepStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.status
	if s.status.Results != nil {
		cp.Results = make(map[string]string, len(s.status.Results))
		for k, v := range s.status.Results {
			cp.Results[k] = v
		}
	}
	return cp
}

func jsonJSONDecode(r io.Reader, dst any) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return jsonUnmarshal(raw, dst)
}

func copyDirContents(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return os.MkdirAll(dst, 0o755)
	}
	return filepath.Walk(src, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode())
	})
}
