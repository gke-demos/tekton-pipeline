package ateactor

import (
	"context"
	"fmt"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ParseEgressSecretURI converts an egressSecret reference ("<secret>" or "<secret>/<key>",
// or an explicit "ate-secret://..." URI) into a Substrate credential_uri.
func ParseEgressSecretURI(namespace, egressSecret string) (string, error) {
	trimmed := strings.TrimSpace(egressSecret)
	if trimmed == "" {
		return "", fmt.Errorf("egressSecret must not be empty")
	}
	if strings.HasPrefix(trimmed, "ate-secret://") {
		return trimmed, nil
	}
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}
	secretName := trimmed
	key := "token"
	if s, k, hasSlash := strings.Cut(trimmed, "/"); hasSlash {
		secretName = strings.TrimSpace(s)
		if strings.TrimSpace(k) != "" {
			key = strings.TrimSpace(k)
		}
	}
	if secretName == "" || strings.Contains(secretName, "/") || strings.Contains(key, "/") {
		return "", fmt.Errorf("invalid egressSecret %q: expected <secret> or <secret>/<key>", egressSecret)
	}
	return fmt.Sprintf("ate-secret://k8s.io/default/%s/%s/%s", ns, secretName, key), nil
}

// DefaultEgressHostPatterns returns the default cluster-local DNS hostname
// patterns for a TaskRun in namespace when EgressHosts is omitted.
func DefaultEgressHostPatterns(namespace string) []string {
	ns := strings.ToLower(strings.TrimSpace(namespace))
	if ns == "" {
		ns = "default"
	}
	return []string{
		"git-oci." + ns + ".svc.cluster.local",
		"git-oci." + ns + ".svc",
		"*." + ns + ".svc.cluster.local",
		"*." + ns + ".svc",
		"*.svc.cluster.local",
		"*.svc",
	}
}

// BuildEgressPolicy constructs the singleton "default" per-Actor EgressPolicy
// injecting Authorization: Bearer <secret> via ate-secret:// on both cleartext
// HTTP (all ports) and HTTPS MITM (:443, :8443).
//
// Note: Substrate admission rejects two rules that tie on the same hostname
// pattern and port key (including both setting Ports.all). Pairing HttpRule on
// all ports with HttpsRule on specific TLS ports (443, 8443) satisfies admission
// while giving HttpsRule port-specificity precedence on 443/8443 and HttpRule
// coverage on all cleartext registry/git ports (80, 5000, 8080, etc.).
func BuildEgressPolicy(atespace, namespace, egressSecret string, egressHosts []string) (*ateapi.EgressPolicy, error) {
	credURI, err := ParseEgressSecretURI(namespace, egressSecret)
	if err != nil {
		return nil, err
	}

	rawHosts := egressHosts
	if len(rawHosts) == 0 {
		rawHosts = DefaultEgressHostPatterns(namespace)
	}
	seen := make(map[string]struct{}, len(rawHosts))
	hostnames := make([]string, 0, len(rawHosts))
	for _, h := range rawHosts {
		norm := strings.ToLower(strings.TrimSpace(h))
		if norm == "" {
			continue
		}
		if _, dup := seen[norm]; dup {
			continue
		}
		seen[norm] = struct{}{}
		hostnames = append(hostnames, norm)
	}
	if len(hostnames) == 0 {
		return nil, fmt.Errorf("at least one egress hostname pattern is required")
	}

	effects := func() *ateapi.HttpRuleEffects {
		return &ateapi.HttpRuleEffects{
			ReplaceHeaders: []*ateapi.CredentialHeader{{
				Header:        "Authorization",
				Prefix:        "Bearer ",
				CredentialUri: credURI,
			}},
		}
	}

	return &ateapi.EgressPolicy{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: atespace,
			Name:     "default",
		},
		Rules: []*ateapi.EgressRule{
			{
				Http: &ateapi.HttpRule{
					Hostnames: hostnames,
					Ports:     &ateapi.Ports{All: &ateapi.AllPorts{}},
					Effects:   effects(),
				},
			},
			{
				Https: &ateapi.HttpsRule{
					Hostnames: append([]string(nil), hostnames...),
					Ports:     &ateapi.Ports{Numbers: []int32{443, 8443}},
					Effects:   effects(),
				},
			},
		},
	}, nil
}

// EnsureActorEgressPolicy idempotently creates the singleton "default" EgressPolicy
// on the specified Actor.
func EnsureActorEgressPolicy(
	ctx context.Context,
	ctrl ateapi.ControlClient,
	atespace, actorName, namespace, egressSecret string,
	egressHosts []string,
) error {
	if strings.TrimSpace(egressSecret) == "" {
		return nil
	}
	policy, err := BuildEgressPolicy(atespace, namespace, egressSecret, egressHosts)
	if err != nil {
		return err
	}
	_, err = ctrl.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{
		Actor:        &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
		EgressPolicy: policy,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create egress policy on actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}
