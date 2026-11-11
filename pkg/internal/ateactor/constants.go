// Package ateactor integrates Tekton PipelineRun and TaskRun reconciliation
// with Agent Substrate v1.0.0 Actors and the Step Supervisor daemon.
package ateactor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	// Environment variable names for in-cluster Substrate configuration.
	EnvSystemNamespace   = "ATE_SYSTEM_NAMESPACE"
	EnvAPIEndpoint       = "ATE_API_ENDPOINT"
	EnvRouterConnectAddr = "ATE_ROUTER_CONNECT_ADDR"
	EnvSupervisorImage   = "ATE_SUPERVISOR_IMAGE"
	EnvTrustBundleFile   = "ATE_TRUST_BUNDLE_FILE"

	DefaultSystemNamespace = "ate-system"
	DefaultClientSA        = "ate-client"
	DefaultRouterClientSA  = "atenet-router"
	DefaultAPIServiceName  = "api"
	DefaultRouterService   = "atenet-router"
	RouterConnectTLSPort   = 8444

	// Annotations recorded on PipelineRun and TaskRun objects.
	AnnotationWorkspaceActor       = "tekton.ate.dev/workspace-actor"
	AnnotationAtespace             = "tekton.ate.dev/atespace"
	AnnotationActorMode            = "tekton.ate.dev/actor-mode"
	AnnotationActorName            = "tekton.ate.dev/actor-name"
	AnnotationActorTemplate        = "tekton.ate.dev/actor-template"
	AnnotationWorkerPod            = "tekton.ate.dev/worker-pod"
	AnnotationSourceTag            = "tekton.ate.dev/source-tag"
	AnnotationMilestoneTag         = "tekton.ate.dev/milestone-tag"
	AnnotationFailureTag           = "tekton.ate.dev/failure-tag"
	AnnotationFailedActor          = "tekton.ate.dev/failed-actor"
	AnnotationRetainWorkspaceActor = "tekton.ate.dev/retain-workspace-actor"
	AnnotationCurrentStepID        = "tekton.ate.dev/current-step-id"
	AnnotationSeededWorkspaces     = "tekton.ate.dev/seeded-workspaces"

	ActorModeTrunk      = "trunk"
	ActorModeBranch     = "branch"
	ActorModeStandalone = "standalone"

	// Canonical 4-volume layout required by validateTemplateVolumesUnchanged.
	VolumeNameWorkspace     = "workspace"
	VolumeNameTekton        = "tekton"
	VolumeNameSupervisorBin = "supervisor-bin"
	VolumeNameAteTrust      = "ate-trust"

	MountPathWorkspace  = "/workspace"
	MountPathTekton     = "/tekton"
	MountPathSupervisor = "/tekton-supervisor"
	MountPathAteTrust   = "/run/ate"

	TrustBundleRelPath  = "trust-bundle.pem"
	TrustBundleFullPath = "/run/ate/trust-bundle.pem"

	TrustBundleEgressMITM  = "egress-mitm.ate.dev"
	TrustBundleSystemRoots = "system-roots.ate.dev"

	SupervisorBinaryPath  = "/tekton-supervisor/ko-app/step-supervisor"
	CoordinatorPort       = 8080
	CoordinatorListenAddr = ":8080"
	WorkerBasePort        = 8090

	DefaultCPUMillicores int64 = 1000
	DefaultMemoryBytes   int64 = 1 << 30 // 1 GiB

	TargetActorHeader = "ate-target-actor"
)

// WorkerLoopbackAddr returns the loopback listen/target address for sibling
// step container index i (1-based: c1 -> "127.0.0.1:8091").
func WorkerLoopbackAddr(containerIndex int) string {
	return fmt.Sprintf("127.0.0.1:%d", WorkerBasePort+containerIndex)
}

// WorkspaceActorName returns the deterministic trunk Workspace Actor name for a PipelineRun.
func WorkspaceActorName(pipelineRunName string) string {
	return SanitizeActorName("ws", pipelineRunName)
}

// StandaloneActorName returns the deterministic Actor name for a standalone TaskRun.
func StandaloneActorName(taskRunName string) string {
	return SanitizeActorName("tr", taskRunName)
}

// BranchActorName returns the deterministic isolated Branch Actor name for a parallel TaskRun.
func BranchActorName(taskRunName string) string {
	return SanitizeActorName("br", taskRunName)
}

// MilestoneTagName returns the deterministic Snapshot Tag name for a PipelineRun fan-out wave.
func MilestoneTagName(pipelineRunName, waveKey string) string {
	if strings.TrimSpace(waveKey) == "" {
		return SanitizeActorName("ms", pipelineRunName)
	}
	return SanitizeActorName("ms", pipelineRunName+"-"+waveKey)
}

// FailureTagName returns the deterministic Snapshot Tag name minted when a TaskRun step fails.
func FailureTagName(taskRunName string) string {
	return SanitizeActorName("failure", taskRunName)
}

// SanitizeActorName constructs a valid <=63-character RFC 1123 DNS label from prefix and rawName.
func SanitizeActorName(prefix, rawName string) string {
	clean := sanitizeDNSLabelChars(strings.ToLower(rawName))
	if clean == "" {
		clean = "actor"
	}
	candidate := prefix + "-" + clean
	candidate = strings.Trim(candidate, "-")
	if len(candidate) <= 63 && candidate == prefix+"-"+ rawName {
		return candidate
	}
	if len(candidate) <= 63 && rawName == clean {
		return candidate
	}
	sum := sha256.Sum256([]byte(prefix + ":" + rawName))
	shortHash := hex.EncodeToString(sum[:])[:10]
	maxPrefixLen := 63 - 1 - len(shortHash)
	base := prefix + "-" + clean
	if len(base) > maxPrefixLen {
		base = strings.TrimRight(base[:maxPrefixLen], "-")
	}
	return base + "-" + shortHash
}

func sanitizeDNSLabelChars(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	lastHyphen := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
			lastHyphen = false
		} else if !lastHyphen && b.Len() > 0 {
			b.WriteByte('-')
			lastHyphen = true
		}
	}
	return strings.Trim(b.String(), "-")
}
