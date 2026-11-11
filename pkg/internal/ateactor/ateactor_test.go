package ateactor_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tektoncd/pipeline/pkg/internal/ateactor"
	"github.com/tektoncd/pipeline/pkg/internal/ateactor/fakecontrol"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	authv1 "k8s.io/api/authentication/v1"
	certsv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const (
	testSupImg    = "gcr.io/demo/step-supervisor@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testGolangImg = "golang@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	testAlpineImg = "alpine@sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func TestSynthesizeTemplateCanonicalVolumesAndDeduplication(t *testing.T) {
	ctx := t.Context()
	rt := fakecontrol.NewTestRuntime(t, testSupImg, map[string]string{
		"golang:1.23": testGolangImg,
		"alpine:3.20": testAlpineImg,
	})
	defer rt.Close()

	synth, err := ateactor.SynthesizeTemplate(ctx, rt, ateactor.TemplateParams{
		Atespace:        "ci",
		SupervisorImage: rt.SupervisorImage(),
		WorkerSelector:  map[string]string{"pool": "gvisor"},
		Steps: []ateactor.StepInput{
			{Name: "clone", Image: "alpine:3.20"},
			{Name: "build", Image: "golang:1.23", CpuMillicores: 2000, MemoryBytes: 2 << 30},
			{Name: "vet", Image: "golang:1.23"},
		},
		Sidecars: []ateactor.SidecarInput{
			{Name: "helper", Image: "alpine:3.20", Command: []string{"/bin/sh", "-c", "sleep 3600"}},
		},
	})
	if err != nil {
		t.Fatalf("SynthesizeTemplate() error: %v", err)
	}

	if !strings.HasPrefix(synth.Template.GetMetadata().GetName(), "tkn-tmpl-") {
		t.Fatalf("unexpected template name %q", synth.Template.GetMetadata().GetName())
	}
	if len(synth.StepContainerIndices) != 3 || synth.StepContainerIndices[0] != 0 || synth.StepContainerIndices[1] != 1 || synth.StepContainerIndices[2] != 1 {
		t.Fatalf("unexpected step container indices: %v", synth.StepContainerIndices)
	}
	if len(synth.Template.GetContainers()) != 3 {
		t.Fatalf("expected 3 containers (c0, c1, s0), got %d", len(synth.Template.GetContainers()))
	}
	c0 := synth.Template.GetContainers()[0]
	c1 := synth.Template.GetContainers()[1]
	s0 := synth.Template.GetContainers()[2]
	if c0.GetName() != "c0" || c0.GetWakeupProbe().GetHttpGet().GetPort() != 8080 {
		t.Fatalf("unexpected c0: %+v", c0)
	}
	if c1.GetName() != "c1" || !slicesContains(c1.GetCommand(), "--listen=127.0.0.1:8091") {
		t.Fatalf("unexpected c1 command: %v", c1.GetCommand())
	}
	if s0.GetName() != "s0" {
		t.Fatalf("unexpected s0 name: %q", s0.GetName())
	}
	if len(synth.Template.GetVolumes()) != 4 {
		t.Fatalf("expected 4 canonical volumes, got %d", len(synth.Template.GetVolumes()))
	}
	if synth.Template.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() != ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		t.Fatalf("expected GOLDEN_SNAPSHOT_MODE_DISABLED, got %v", synth.Template.GetSnapshotConfig())
	}
	if synth.Template.GetResources().GetLimits().GetCpuMillicores() != 2000 {
		t.Fatalf("expected derived CPU limit 2000m, got %d", synth.Template.GetResources().GetLimits().GetCpuMillicores())
	}
}

func TestActorHandoffPreservesWorkspaceAndAppliesEgressAndSourceTag(t *testing.T) {
	ctx := t.Context()
	rt := fakecontrol.NewTestRuntime(t, testSupImg, map[string]string{
		"golang:1.23": testGolangImg,
		"alpine:3.20": testAlpineImg,
	})
	defer rt.Close()

	// Task A uses golang:1.23
	synthA, err := ateactor.SynthesizeTemplate(ctx, rt, ateactor.TemplateParams{
		Atespace:        "ci",
		SupervisorImage: rt.SupervisorImage(),
		Steps:           []ateactor.StepInput{{Name: "build", Image: "golang:1.23"}},
	})
	if err != nil {
		t.Fatalf("SynthesizeTemplate A: %v", err)
	}
	storedTmplA, err := ateactor.EnsureActorTemplate(ctx, rt.Control(), synthA.Template)
	if err != nil {
		t.Fatalf("EnsureActorTemplate A: %v", err)
	}

	actor, err := ateactor.EnsureActorReady(ctx, rt.Control(), ateactor.EnsureActorParams{
		Atespace:     "ci",
		ActorName:    "ws-demo",
		Template:     storedTmplA,
		Namespace:    "ci-ns",
		EgressSecret: "git-creds/token",
	})
	if err != nil {
		t.Fatalf("EnsureActorReady A: %v", err)
	}
	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("expected RUNNING actor, got %v", actor.GetStatus().GetState())
	}

	// Verify EgressPolicy was created with ate-secret:// URI.
	pol, err := rt.Control().GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{
		Actor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-demo"},
		Name:  "default",
	})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if len(pol.GetRules()) != 2 {
		t.Fatalf("expected 2 egress rules (http + https), got %d", len(pol.GetRules()))
	}
	gotURI := pol.GetRules()[0].GetHttp().GetEffects().GetReplaceHeaders()[0].GetCredentialUri()
	wantURI := "ate-secret://k8s.io/default/ci-ns/git-creds/token"
	if gotURI != wantURI {
		t.Fatalf("CredentialUri = %q, want %q", gotURI, wantURI)
	}

	// Write a workspace file and a >4 KiB result in Task A.
	sup := rt.SupervisorClient("ci", "ws-demo")
	st, err := sup.RunStep(ctx, ateactor.RunStepRequest{
		StepID:      "step-a-0",
		Name:        "build",
		WorkingDir:  "/workspace/source",
		Script:      "mkdir -p /workspace/source && echo -n 'built-binary-v1' > /workspace/source/app.bin && head -c 6000 /dev/zero | tr '\\0' 'x' > /tekton/results/big",
		ResultNames: []string{"big"},
	})
	if err != nil {
		t.Fatalf("RunStep A: %v", err)
	}
	st, err = sup.WaitStep(ctx, st.StepID, 5*time.Second)
	if err != nil {
		t.Fatalf("WaitStep A: %v", err)
	}
	if st.State != ateactor.StepStateCompleted || len(st.Results["big"]) != 6000 {
		t.Fatalf("unexpected Step A status: state=%s resultLen=%d err=%s", st.State, len(st.Results["big"]), st.Error)
	}

	if err := ateactor.SuspendActorSync(ctx, rt.Control(), "ci", "ws-demo"); err != nil {
		t.Fatalf("SuspendActorSync A: %v", err)
	}

	// Mint a Snapshot Tag on ws-demo while it uses tmplA, so we can also test SourceTag cross-template seeding.
	_, err = rt.Control().CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: "ci", Name: "ms-post-build"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: "ci", Name: "ws-demo"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	// Task B uses alpine:3.20 (different ActorTemplate!) on the SAME trunk Workspace Actor ws-demo.
	synthB, err := ateactor.SynthesizeTemplate(ctx, rt, ateactor.TemplateParams{
		Atespace:        "ci",
		SupervisorImage: rt.SupervisorImage(),
		Steps:           []ateactor.StepInput{{Name: "package", Image: "alpine:3.20"}},
	})
	if err != nil {
		t.Fatalf("SynthesizeTemplate B: %v", err)
	}
	storedTmplB, err := ateactor.EnsureActorTemplate(ctx, rt.Control(), synthB.Template)
	if err != nil {
		t.Fatalf("EnsureActorTemplate B: %v", err)
	}

	actorB, err := ateactor.EnsureActorReady(ctx, rt.Control(), ateactor.EnsureActorParams{
		Atespace:  "ci",
		ActorName: "ws-demo",
		Template:  storedTmplB,
	})
	if err != nil {
		t.Fatalf("EnsureActorReady B (UpdateActor repoint): %v", err)
	}
	if actorB.GetActorTemplate().GetName() != storedTmplB.GetMetadata().GetName() {
		t.Fatalf("expected actor_template %q, got %q", storedTmplB.GetMetadata().GetName(), actorB.GetActorTemplate().GetName())
	}

	stB, err := sup.RunStep(ctx, ateactor.RunStepRequest{
		StepID:      "step-b-0",
		Name:        "package",
		WorkingDir:  "/workspace/source",
		Script:      "cat /workspace/source/app.bin > /tekton/results/inherited",
		ResultNames: []string{"inherited"},
	})
	if err != nil {
		t.Fatalf("RunStep B: %v", err)
	}
	stB, err = sup.WaitStep(ctx, stB.StepID, 5*time.Second)
	if err != nil {
		t.Fatalf("WaitStep B: %v", err)
	}
	if stB.Results["inherited"] != "built-binary-v1" {
		t.Fatalf("expected inherited workspace file 'built-binary-v1', got %q", stB.Results["inherited"])
	}

	// Also verify creating a brand-new Actor seeded from SourceTag ("ms-post-build" taken on tmplA) using tmplB!
	branched, err := ateactor.EnsureActorReady(ctx, rt.Control(), ateactor.EnsureActorParams{
		Atespace:  "ci",
		ActorName: "br-test",
		Template:  storedTmplB,
		SourceTag: "ms-post-build",
	})
	if err != nil {
		t.Fatalf("EnsureActorReady from SourceTag across templates: %v", err)
	}
	if branched.GetActorTemplate().GetName() != storedTmplB.GetMetadata().GetName() {
		t.Fatalf("expected branched actor on tmplB, got %q", branched.GetActorTemplate().GetName())
	}
	supBr := rt.SupervisorClient("ci", "br-test")
	stBr, err := supBr.RunStep(ctx, ateactor.RunStepRequest{
		StepID:      "step-br-0",
		Script:      "cat /workspace/source/app.bin > /tekton/results/fromtag",
		ResultNames: []string{"fromtag"},
	})
	if err != nil {
		t.Fatalf("RunStep Br: %v", err)
	}
	stBr, err = supBr.WaitStep(ctx, stBr.StepID, 5*time.Second)
	if err != nil {
		t.Fatalf("WaitStep Br: %v", err)
	}
	if stBr.Results["fromtag"] != "built-binary-v1" {
		t.Fatalf("expected SourceTag-seeded file 'built-binary-v1', got %q", stBr.Results["fromtag"])
	}
}

func TestClusterRuntimeTLSBearerAndConnectTunnel(t *testing.T) {
	ctx := t.Context()

	// 1. Mint test CA, server certs for api.ate-system.svc and atenet-router.ate-system.svc, and signer key for PodCertificateRequests.
	caKey, caCert, caPEM := mustSelfSignedCA(t)
	apiTLS := mustLeafCert(t, caKey, caCert, []string{"api.ate-system.svc"})
	routerTLS := mustLeafCert(t, caKey, caCert, []string{"atenet-router.ate-system.svc"})

	// 2. Fake Kubernetes clientset serving ClusterTrustBundle, TokenRequest, and PodCertificateRequest.
	kc := fakek8s.NewSimpleClientset(&certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "servicedns.podcert.ate.dev:identity:live",
			Labels: map[string]string{"podcert.ate.dev/canarying": "live"},
		},
		Spec: certsv1.ClusterTrustBundleSpec{
			SignerName:  "servicedns.podcert.ate.dev/identity",
			TrustBundle: caPEM,
		},
	})
	kc.Resources = []*metav1.APIResourceList{{
		GroupVersion: "certificates.k8s.io/v1",
		APIResources: []metav1.APIResource{
			{Name: "clustertrustbundles", Kind: "ClusterTrustBundle"},
			{Name: "podcertificaterequests", Namespaced: true, Kind: "PodCertificateRequest"},
		},
	}}
	kc.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		createAct := action.(ktesting.CreateAction)
		tr := createAct.GetObject().(*authv1.TokenRequest)
		tr.Status = authv1.TokenRequestStatus{
			Token:               "minted-jwt-for-" + strings.Join(tr.Spec.Audiences, ","),
			ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Hour)),
		}
		return true, tr, nil
	})
	var issuedPCRName string
	var issuedPCRChain string
	kc.PrependReactor("create", "podcertificaterequests", func(action ktesting.Action) (bool, runtime.Object, error) {
		createAct := action.(ktesting.CreateAction)
		pcr := createAct.GetObject().(*certsv1.PodCertificateRequest).DeepCopy()
		if pcr.Name == "" {
			pcr.Name = pcr.GenerateName + "1"
		}
		csr, err := x509.ParseCertificateRequest(pcr.Spec.StubPKCS10Request)
		if err != nil {
			return true, nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(42),
			Subject:      pkix.Name{CommonName: pcr.Spec.ServiceAccountName},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, csr.PublicKey, caKey)
		if err != nil {
			return true, nil, err
		}
		issuedPCRName = pcr.Name
		issuedPCRChain = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		pcr.Status.CertificateChain = issuedPCRChain
		pcr.Status.Conditions = []metav1.Condition{{
			Type:   certsv1.PodCertificateRequestConditionTypeIssued,
			Status: metav1.ConditionTrue,
		}}
		return true, pcr, nil
	})
	kc.PrependReactor("get", "podcertificaterequests", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{Name: issuedPCRName, Namespace: "ate-system"},
			Status: certsv1.PodCertificateRequestStatus{
				CertificateChain: issuedPCRChain,
				Conditions: []metav1.Condition{{
					Type:   certsv1.PodCertificateRequestConditionTypeIssued,
					Status: metav1.ConditionTrue,
				}},
			},
		}, nil
	})

	// 3. Start local TLS gRPC server verifying Authorization: Bearer header.
	fakeCtrl := fakecontrol.NewServer(t)
	var sawAuthHeader atomic.Value
	grpcSrv := grpc.NewServer(
		grpc.Creds(credentials.NewServerTLSFromCert(&apiTLS)),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if vals := md.Get("authorization"); len(vals) > 0 {
					sawAuthHeader.Store(vals[0])
				}
			}
			return handler(ctx, req)
		}),
	)
	ateapi.RegisterControlServer(grpcSrv, fakeCtrl)
	apiLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen api: %v", err)
	}
	defer grpcSrv.Stop()
	go func() { _ = grpcSrv.Serve(apiLis) }()

	// 4. Start local Step Supervisor HTTP backend + TLS CONNECT router requiring mTLS client cert and ate-target-actor header.
	supBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer supBackend.Close()

	clientPool := x509.NewCertPool()
	clientPool.AddCert(caCert)
	routerLis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{routerTLS},
		ClientCAs:    clientPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("listen router: %v", err)
	}
	defer routerLis.Close()
	var sawTargetHeader atomic.Value
	go func() {
		for {
			conn, acceptErr := routerLis.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				tlsConn := c.(*tls.Conn)
				if err := tlsConn.Handshake(); err != nil {
					return
				}
				var buf [1024]byte
				n, _ := tlsConn.Read(buf[:])
				reqText := string(buf[:n])
				for _, line := range strings.Split(reqText, "\r\n") {
					if strings.HasPrefix(strings.ToLower(line), "ate-target-actor:") {
						sawTargetHeader.Store(strings.TrimSpace(strings.SplitN(line, ":", 2)[1]))
					}
				}
				backendAddr := strings.TrimPrefix(supBackend.URL, "http://")
				up, dialErr := net.Dial("tcp", backendAddr)
				if dialErr != nil {
					return
				}
				defer up.Close()
				_, _ = io.WriteString(tlsConn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(up, tlsConn); done <- struct{}{} }()
				go func() { _, _ = io.Copy(tlsConn, up); done <- struct{}{} }()
				<-done
			}(conn)
		}
	}()

	rt, err := ateactor.NewClusterRuntime(ctx, kc, ateactor.ClusterConfig{
		SystemNamespace:   "ate-system",
		APIEndpoint:       apiLis.Addr().String(),
		RouterConnectAddr: routerLis.Addr().String(),
		SupervisorImage:   testSupImg,
	})
	if err != nil {
		t.Fatalf("NewClusterRuntime: %v", err)
	}
	defer rt.Close()

	if err := ateactor.EnsureAtespace(ctx, rt.Control(), "ci"); err != nil {
		t.Fatalf("EnsureAtespace over TLS gRPC: %v", err)
	}
	if got, _ := sawAuthHeader.Load().(string); got != "Bearer minted-jwt-for-api.ate-system.svc" {
		t.Fatalf("unexpected gRPC Authorization header: %q", got)
	}

	if err := rt.SupervisorClient("ci", "ws-pr1").Ready(ctx); err != nil {
		t.Fatalf("SupervisorClient.Ready over CONNECT-TLS tunnel: %v", err)
	}
	if got, _ := sawTargetHeader.Load().(string); got != "ci/ws-pr1" {
		t.Fatalf("unexpected ate-target-actor header: %q", got)
	}
}

func slicesContains(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}

func mustSelfSignedCA(t *testing.T) (*ecdsa.PrivateKey, *x509.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ate-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pemStr := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	return key, cert, pemStr
}

func mustLeafCert(t *testing.T, caKey *ecdsa.PrivateKey, caCert *x509.Certificate, dnsNames []string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(int64(len(dnsNames) + 10)),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("leaf-%s", dnsNames[0])},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}
}
