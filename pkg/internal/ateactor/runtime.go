package ateactor

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	authv1 "k8s.io/api/authentication/v1"
	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	serviceDNSSignerName = "servicedns.podcert.ate.dev/identity"
	liveBundleSelector   = "podcert.ate.dev/canarying=live"
	podIdentitySigner    = "podidentity.podcert.ate.dev/identity"
	routerCertRequester  = "tekton-pipelines-controller"

	roundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`
)

type runtimeContextKey struct{}

// WithRuntime attaches an explicit Runtime to ctx (used by reconciler tests).
func WithRuntime(ctx context.Context, rt Runtime) context.Context {
	return context.WithValue(ctx, runtimeContextKey{}, rt)
}

// FromContext extracts a Runtime previously attached via WithRuntime, if any.
func FromContext(ctx context.Context) Runtime {
	if rt, ok := ctx.Value(runtimeContextKey{}).(Runtime); ok {
		return rt
	}
	return nil
}

// Runtime abstracts control-plane (ate-api-server gRPC) and data-plane
// (atenet-router Step Supervisor HTTP) communication for Tekton reconcilers.
type Runtime interface {
	Control() ateapi.ControlClient
	SupervisorClient(atespace, actorName string) *SupervisorClient
	ResolveImageDigest(ctx context.Context, ref string) (string, error)
	SupervisorImage() string
	Close() error
}

// ClusterConfig configures NewClusterRuntime. Empty fields take defaults from
// environment variables and standard Substrate install names.
type ClusterConfig struct {
	SystemNamespace   string
	APIEndpoint       string
	RouterConnectAddr string
	SupervisorImage   string
	TrustBundleFile   string
}

// ClusterConfigFromEnv populates ClusterConfig from environment variables.
func ClusterConfigFromEnv() ClusterConfig {
	ns := strings.TrimSpace(os.Getenv(EnvSystemNamespace))
	if ns == "" {
		ns = DefaultSystemNamespace
	}
	apiEndpoint := strings.TrimSpace(os.Getenv(EnvAPIEndpoint))
	if apiEndpoint == "" {
		apiEndpoint = fmt.Sprintf("%s.%s.svc:443", DefaultAPIServiceName, ns)
	}
	routerAddr := strings.TrimSpace(os.Getenv(EnvRouterConnectAddr))
	if routerAddr == "" {
		routerAddr = fmt.Sprintf("%s.%s.svc:%d", DefaultRouterService, ns, RouterConnectTLSPort)
	}
	return ClusterConfig{
		SystemNamespace:   ns,
		APIEndpoint:       apiEndpoint,
		RouterConnectAddr: routerAddr,
		SupervisorImage:   strings.TrimSpace(os.Getenv(EnvSupervisorImage)),
		TrustBundleFile:   strings.TrimSpace(os.Getenv(EnvTrustBundleFile)),
	}
}

// ClusterRuntime implements Runtime for in-cluster execution against a live
// Agent Substrate installation.
type ClusterRuntime struct {
	conn            *grpc.ClientConn
	control         ateapi.ControlClient
	routerAddr      string
	routerTLS       *tls.Config
	digestResolver  *CachingDigestResolver
	supervisorImage string
}

// NewClusterRuntime dials ate-api-server over TLS with refreshed Kubernetes
// TokenRequest credentials and prepares an mTLS CONNECT-TLS dialer to
// atenet-router :8444 authenticated via PodCertificateRequest.
func NewClusterRuntime(ctx context.Context, kc kubernetes.Interface, cfg ClusterConfig) (*ClusterRuntime, error) {
	if kc == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	envCfg := ClusterConfigFromEnv()
	if cfg.SystemNamespace == "" {
		cfg.SystemNamespace = envCfg.SystemNamespace
	}
	if cfg.APIEndpoint == "" {
		cfg.APIEndpoint = fmt.Sprintf("%s.%s.svc:443", DefaultAPIServiceName, cfg.SystemNamespace)
	}
	if cfg.RouterConnectAddr == "" {
		cfg.RouterConnectAddr = fmt.Sprintf("%s.%s.svc:%d", DefaultRouterService, cfg.SystemNamespace, RouterConnectTLSPort)
	}
	if cfg.SupervisorImage == "" {
		cfg.SupervisorImage = envCfg.SupervisorImage
	}
	if cfg.TrustBundleFile == "" {
		cfg.TrustBundleFile = envCfg.TrustBundleFile
	}

	rootPool, err := loadServiceDNSTrustPool(ctx, kc, cfg.TrustBundleFile)
	if err != nil {
		return nil, err
	}

	apiServerName := fmt.Sprintf("%s.%s.svc", DefaultAPIServiceName, cfg.SystemNamespace)
	apiTLS := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    rootPool,
		ServerName: apiServerName,
	}

	tokenCreds := &refreshingSATokenCreds{
		kc:              kc,
		systemNamespace: cfg.SystemNamespace,
		serviceAccount:  DefaultClientSA,
		audience:        apiServerName,
	}
	if _, err := tokenCreds.token(ctx); err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(
		cfg.APIEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(apiTLS)),
		grpc.WithDefaultServiceConfig(roundRobinServiceConfig),
		grpc.WithPerRPCCredentials(tokenCreds),
	)
	if err != nil {
		return nil, fmt.Errorf("dial ate-api-server %q: %w", cfg.APIEndpoint, err)
	}

	routerServerName := fmt.Sprintf("%s.%s.svc", DefaultRouterService, cfg.SystemNamespace)
	clientCerts := &refreshingRouterClientCerts{
		kc:              kc,
		systemNamespace: cfg.SystemNamespace,
		serviceAccount:  DefaultRouterClientSA,
	}
	if _, err := clientCerts.get(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	routerTLS := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    rootPool,
		ServerName: routerServerName,
		GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return clientCerts.get(info.Context())
		},
	}

	return &ClusterRuntime{
		conn:            conn,
		control:         ateapi.NewControlClient(conn),
		routerAddr:      cfg.RouterConnectAddr,
		routerTLS:       routerTLS,
		digestResolver:  NewCachingDigestResolver(kc),
		supervisorImage: cfg.SupervisorImage,
	}, nil
}

func (r *ClusterRuntime) Control() ateapi.ControlClient {
	return r.control
}

func (r *ClusterRuntime) SupervisorImage() string {
	return r.supervisorImage
}

func (r *ClusterRuntime) ResolveImageDigest(ctx context.Context, ref string) (string, error) {
	return r.digestResolver.ResolveImageDigest(ctx, ref)
}

func (r *ClusterRuntime) SupervisorClient(atespace, actorName string) *SupervisorClient {
	targetRef := atespace + "/" + actorName
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialRouterConnect(ctx, r.routerAddr, r.routerTLS, actorName, CoordinatorPort, targetRef)
		},
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     30 * time.Second,
	}
	baseURL := fmt.Sprintf("http://%s:%d", actorName, CoordinatorPort)
	return NewSupervisorClient(baseURL, &http.Client{Transport: transport})
}

func (r *ClusterRuntime) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func dialRouterConnect(
	ctx context.Context,
	routerAddr string,
	tlsCfg *tls.Config,
	actorName string,
	port int,
	targetActorRef string,
) (net.Conn, error) {
	var d net.Dialer
	rawConn, err := d.DialContext(ctx, "tcp", routerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial atenet-router %q: %w", routerAddr, err)
	}
	tlsConn := tls.Client(rawConn, tlsCfg.Clone())
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("TLS handshake with atenet-router %q: %w", routerAddr, err)
	}

	stopOnCancel := context.AfterFunc(ctx, func() { _ = tlsConn.Close() })
	defer stopOnCancel()

	dest := net.JoinHostPort(actorName, strconv.Itoa(port))
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: dest},
		Host:   dest,
		Header: http.Header{
			TargetActorHeader: []string{targetActorRef},
		},
	}
	if err := req.Write(tlsConn); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("write CONNECT to %s: %w", dest, err)
	}

	reader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("read CONNECT response from %s: %w", dest, err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = tlsConn.Close()
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return nil, fmt.Errorf("atenet-router rejected CONNECT to %s (%s): %s", targetActorRef, resp.Status, msg)
	}
	if !stopOnCancel() {
		_ = tlsConn.Close()
		return nil, ctx.Err()
	}
	return &bufferedConn{Conn: tlsConn, reader: reader}, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func loadServiceDNSTrustPool(ctx context.Context, kc kubernetes.Interface, fallbackPEMFile string) (*x509.CertPool, error) {
	if fallbackPEMFile != "" {
		raw, err := os.ReadFile(fallbackPEMFile)
		if err != nil {
			return nil, fmt.Errorf("read trust bundle file %q: %w", fallbackPEMFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("trust bundle file %q contains no valid certificates", fallbackPEMFile)
		}
		return pool, nil
	}

	pems, err := listServiceDNSClusterTrustBundles(ctx, kc)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	for _, pemBytes := range pems {
		if !pool.AppendCertsFromPEM([]byte(pemBytes)) {
			return nil, fmt.Errorf("ClusterTrustBundle for %q contained invalid PEM", serviceDNSSignerName)
		}
	}
	return pool, nil
}

func listServiceDNSClusterTrustBundles(ctx context.Context, kc kubernetes.Interface) ([]string, error) {
	opts := metav1.ListOptions{LabelSelector: liveBundleSelector}
	for _, ver := range []string{"v1", "v1beta1", "v1alpha1"} {
		gv := "certificates.k8s.io/" + ver
		resList, err := kc.Discovery().ServerResourcesForGroupVersion(gv)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("discover %s: %w", gv, err)
		}
		served := false
		for _, r := range resList.APIResources {
			if r.Name == "clustertrustbundles" {
				served = true
				break
			}
		}
		if !served {
			continue
		}

		var out []string
		switch ver {
		case "v1":
			list, listErr := kc.CertificatesV1().ClusterTrustBundles().List(ctx, opts)
			if listErr != nil {
				return nil, fmt.Errorf("list v1 ClusterTrustBundles: %w", listErr)
			}
			for _, item := range list.Items {
				if item.Spec.SignerName == serviceDNSSignerName {
					out = append(out, item.Spec.TrustBundle)
				}
			}
		case "v1beta1":
			list, listErr := kc.CertificatesV1beta1().ClusterTrustBundles().List(ctx, opts)
			if listErr != nil {
				return nil, fmt.Errorf("list v1beta1 ClusterTrustBundles: %w", listErr)
			}
			for _, item := range list.Items {
				if item.Spec.SignerName == serviceDNSSignerName {
					out = append(out, item.Spec.TrustBundle)
				}
			}
		case "v1alpha1":
			list, listErr := kc.CertificatesV1alpha1().ClusterTrustBundles().List(ctx, opts)
			if listErr != nil {
				return nil, fmt.Errorf("list v1alpha1 ClusterTrustBundles: %w", listErr)
			}
			for _, item := range list.Items {
				if item.Spec.SignerName == serviceDNSSignerName {
					out = append(out, item.Spec.TrustBundle)
				}
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no live ClusterTrustBundle found for signer %q", serviceDNSSignerName)
		}
		return out, nil
	}
	return nil, fmt.Errorf("cluster serves none of v1, v1beta1, or v1alpha1 ClusterTrustBundle")
}

type refreshingSATokenCreds struct {
	kc              kubernetes.Interface
	systemNamespace string
	serviceAccount  string
	audience        string

	mu        sync.Mutex
	cachedJWT string
	refreshAt time.Time
}

func (c *refreshingSATokenCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	jwt, err := c.token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + jwt}, nil
}

func (c *refreshingSATokenCreds) RequireTransportSecurity() bool { return true }

func (c *refreshingSATokenCreds) token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if c.cachedJWT != "" && now.Before(c.refreshAt) {
		return c.cachedJWT, nil
	}

	expSeconds := int64(3600)
	tr := &authv1.TokenRequest{
		Spec: authv1.TokenRequestSpec{
			Audiences:         []string{c.audience},
			ExpirationSeconds: &expSeconds,
		},
	}
	resp, err := c.kc.CoreV1().ServiceAccounts(c.systemNamespace).CreateToken(ctx, c.serviceAccount, tr, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("mint bearer token for %s/%s: %w", c.systemNamespace, c.serviceAccount, err)
	}
	if resp.Status.Token == "" {
		return "", fmt.Errorf("minted bearer token for %s/%s was empty", c.systemNamespace, c.serviceAccount)
	}
	c.cachedJWT = resp.Status.Token
	exp := resp.Status.ExpirationTimestamp.Time
	lifetime := exp.Sub(now)
	if lifetime <= 0 {
		lifetime = time.Hour
	}
	c.refreshAt = now.Add(lifetime / 2)
	return c.cachedJWT, nil
}

type refreshingRouterClientCerts struct {
	kc              kubernetes.Interface
	systemNamespace string
	serviceAccount  string

	mu        sync.Mutex
	cert      *tls.Certificate
	refreshAt time.Time
}

func (c *refreshingRouterClientCerts) get(ctx context.Context) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if c.cert != nil && now.Before(c.refreshAt) {
		return c.cert, nil
	}
	timeout := 2 * time.Minute
	if c.cert != nil && c.cert.Leaf != nil && now.Before(c.cert.Leaf.NotAfter) {
		timeout = 15 * time.Second
	}
	cert, err := requestPodIdentityCert(ctx, c.kc, c.systemNamespace, c.serviceAccount, timeout)
	if err != nil {
		if c.cert != nil && c.cert.Leaf != nil && time.Now().Before(c.cert.Leaf.NotAfter) {
			c.refreshAt = time.Now().Add(time.Minute)
			return c.cert, nil
		}
		return nil, err
	}
	c.cert = &cert
	c.refreshAt = cert.Leaf.NotBefore.Add(cert.Leaf.NotAfter.Sub(cert.Leaf.NotBefore) / 2)
	return c.cert, nil
}

func requestPodIdentityCert(
	ctx context.Context,
	kc kubernetes.Interface,
	namespace, serviceAccount string,
	timeout time.Duration,
) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate client ECDSA key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create stub CSR: %w", err)
	}
	maxExp := int32(24 * 60 * 60)

	useV1, err := discoverPodCertificateRequestV1(kc)
	if err != nil {
		return tls.Certificate{}, err
	}

	var name string
	if useV1 {
		created, createErr := kc.CertificatesV1().PodCertificateRequests(namespace).Create(ctx, &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{GenerateName: routerCertRequester + "-", Namespace: namespace},
			Spec: certsv1.PodCertificateRequestSpec{
				SignerName:           podIdentitySigner,
				PodName:              routerCertRequester,
				PodUID:               uuid.NewUUID(),
				ServiceAccountName:   serviceAccount,
				ServiceAccountUID:    uuid.NewUUID(),
				NodeName:             types.NodeName(routerCertRequester),
				NodeUID:              uuid.NewUUID(),
				MaxExpirationSeconds: &maxExp,
				StubPKCS10Request:    csr,
			},
		}, metav1.CreateOptions{})
		if createErr != nil {
			return tls.Certificate{}, fmt.Errorf("create v1 PodCertificateRequest: %w", createErr)
		}
		name = created.Name
		defer func() {
			delCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = kc.CertificatesV1().PodCertificateRequests(namespace).Delete(delCtx, name, metav1.DeleteOptions{})
		}()
	} else {
		created, createErr := kc.CertificatesV1beta1().PodCertificateRequests(namespace).Create(ctx, &certsv1beta1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{GenerateName: routerCertRequester + "-", Namespace: namespace},
			Spec: certsv1beta1.PodCertificateRequestSpec{
				SignerName:           podIdentitySigner,
				PodName:              routerCertRequester,
				PodUID:               uuid.NewUUID(),
				ServiceAccountName:   serviceAccount,
				ServiceAccountUID:    uuid.NewUUID(),
				NodeName:             types.NodeName(routerCertRequester),
				NodeUID:              uuid.NewUUID(),
				MaxExpirationSeconds: &maxExp,
				StubPKCS10Request:    csr,
			},
		}, metav1.CreateOptions{})
		if createErr != nil {
			return tls.Certificate{}, fmt.Errorf("create v1beta1 PodCertificateRequest: %w", createErr)
		}
		name = created.Name
		defer func() {
			delCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = kc.CertificatesV1beta1().PodCertificateRequests(namespace).Delete(delCtx, name, metav1.DeleteOptions{})
		}()
	}

	var chainPEM string
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		var status certsv1.PodCertificateRequestStatus
		if useV1 {
			pcr, getErr := kc.CertificatesV1().PodCertificateRequests(namespace).Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			status = pcr.Status
		} else {
			pcr, getErr := kc.CertificatesV1beta1().PodCertificateRequests(namespace).Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			status = certsv1.PodCertificateRequestStatus(pcr.Status)
		}
		for _, c := range status.Conditions {
			if c.Status != metav1.ConditionTrue {
				continue
			}
			switch c.Type {
			case certsv1.PodCertificateRequestConditionTypeIssued:
				chainPEM = status.CertificateChain
				return true, nil
			case certsv1.PodCertificateRequestConditionTypeDenied, certsv1.PodCertificateRequestConditionTypeFailed:
				return false, fmt.Errorf("%s: %s: %s", c.Type, c.Reason, c.Message)
			}
		}
		return false, nil
	})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("wait for PodCertificateRequest %s/%s: %w", namespace, name, err)
	}

	cert := tls.Certificate{PrivateKey: key}
	for rest := []byte(chainPEM); ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		cert.Certificate = append(cert.Certificate, block.Bytes)
	}
	if len(cert.Certificate) == 0 {
		return tls.Certificate{}, fmt.Errorf("PodCertificateRequest %s/%s issued empty certificate chain", namespace, name)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse issued leaf certificate: %w", err)
	}
	cert.Leaf = leaf
	return cert, nil
}

func discoverPodCertificateRequestV1(kc kubernetes.Interface) (bool, error) {
	for _, ver := range []string{"v1", "v1beta1"} {
		resList, err := kc.Discovery().ServerResourcesForGroupVersion("certificates.k8s.io/" + ver)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("discover PodCertificateRequest %s: %w", ver, err)
		}
		for _, r := range resList.APIResources {
			if r.Name == "podcertificaterequests" {
				return ver == "v1", nil
			}
		}
	}
	return false, fmt.Errorf("cluster serves neither v1 nor v1beta1 PodCertificateRequest")
}
