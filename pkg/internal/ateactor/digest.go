package ateactor

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"k8s.io/client-go/kubernetes"
)

// ImageDigestResolver resolves an OCI image reference to a digest-pinned
// reference (<repo>@sha256:...).
type ImageDigestResolver interface {
	ResolveImageDigest(ctx context.Context, ref string) (string, error)
}

// CachingDigestResolver resolves OCI image tags to @sha256:... digests using
// go-containerregistry crane and caches resolved digests in memory.
type CachingDigestResolver struct {
	kc kubernetes.Interface

	mu    sync.RWMutex
	cache map[string]string
}

// NewCachingDigestResolver constructs an in-memory caching digest resolver.
func NewCachingDigestResolver(kc kubernetes.Interface) *CachingDigestResolver {
	return &CachingDigestResolver{
		kc:    kc,
		cache: make(map[string]string),
	}
}

// HasPinnedDigest reports whether ref already carries a valid @sha256:... digest.
func HasPinnedDigest(ref string) bool {
	_, err := name.NewDigest(strings.TrimSpace(ref))
	return err == nil
}

// ResolveImageDigest returns ref unchanged if it already carries @sha256:...,
// or resolves its remote manifest digest via crane and returns <repo>@<digest>.
func (r *CachingDigestResolver) ResolveImageDigest(ctx context.Context, ref string) (string, error) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return "", fmt.Errorf("image reference must not be empty")
	}
	if _, err := name.NewDigest(trimmed); err == nil {
		return trimmed, nil
	}

	r.mu.RLock()
	if pinned, ok := r.cache[trimmed]; ok {
		r.mu.RUnlock()
		return pinned, nil
	}
	r.mu.RUnlock()

	parsedRef, err := name.ParseReference(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse image reference %q: %w", trimmed, err)
	}

	opts := []crane.Option{crane.WithContext(ctx)}
	if r.kc != nil {
		kcAuth, kcErr := k8schain.New(ctx, r.kc, k8schain.Options{})
		if kcErr == nil {
			opts = append(opts, crane.WithAuthFromKeychain(authn.NewMultiKeychain(kcAuth, authn.DefaultKeychain)))
		} else {
			opts = append(opts, crane.WithAuthFromKeychain(authn.DefaultKeychain))
		}
	} else {
		opts = append(opts, crane.WithAuthFromKeychain(authn.DefaultKeychain))
	}

	if isClusterLocalRegistry(parsedRef.Context().RegistryStr()) {
		opts = append(opts, crane.Insecure)
		if insecureRef, insecErr := name.ParseReference(trimmed, name.Insecure); insecErr == nil {
			parsedRef = insecureRef
		}
	}

	digest, err := crane.Digest(parsedRef.String(), opts...)
	if err != nil {
		return "", fmt.Errorf("resolve digest for %q: %w", trimmed, err)
	}
	pinned := parsedRef.Context().Name() + "@" + digest

	r.mu.Lock()
	r.cache[trimmed] = pinned
	r.mu.Unlock()
	return pinned, nil
}

func isClusterLocalRegistry(host string) bool {
	h := strings.ToLower(host)
	return strings.HasSuffix(h, ".svc") ||
		strings.HasSuffix(h, ".svc.cluster.local") ||
		strings.Contains(h, ".svc:") ||
		strings.Contains(h, ".svc.cluster.local:") ||
		strings.HasPrefix(h, "127.0.0.1:") ||
		strings.HasPrefix(h, "localhost:")
}
