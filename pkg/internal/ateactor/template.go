package ateactor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// StepInput describes the image and optional compute limits of a Tekton step
// for ActorTemplate synthesis.
type StepInput struct {
	Name          string
	Image         string
	CpuMillicores int64
	MemoryBytes   int64
}

// SidecarInput describes a Tekton sidecar container for ActorTemplate synthesis.
type SidecarInput struct {
	Name          string
	Image         string
	Command       []string
	Args          []string
	Script        string
	Env           []EnvEntry
	CpuMillicores int64
	MemoryBytes   int64
}

// TemplateParams holds inputs for SynthesizeTemplate.
type TemplateParams struct {
	Atespace        string
	SupervisorImage string
	WorkerSelector  map[string]string
	Steps           []StepInput
	Sidecars        []SidecarInput
}

// SynthesizedTemplate contains the content-addressed ActorTemplate and the
// per-step mapping onto its deduplicated step containers.
type SynthesizedTemplate struct {
	Template             *ateapi.ActorTemplate
	StepContainerIndices []int
	StepContainerImages  []string
}

// CanonicalVolumes returns the exact 4-volume slice required on every Tekton
// ActorTemplate so UpdateActor always passes validateTemplateVolumesUnchanged.
func CanonicalVolumes(pinnedSupervisorImage string) []*ateapi.Volume {
	return []*ateapi.Volume{
		{
			Name:       VolumeNameWorkspace,
			DurableDir: &ateapi.DurableDirVolumeSource{},
		},
		{
			Name:       VolumeNameTekton,
			DurableDir: &ateapi.DurableDirVolumeSource{},
		},
		{
			Name: VolumeNameSupervisorBin,
			Image: &ateapi.ImageVolumeSource{
				Reference: pinnedSupervisorImage,
			},
		},
		{
			Name: VolumeNameAteTrust,
			SystemInfo: &ateapi.SystemInfoVolumeSource{
				DataSources: []*ateapi.SystemInfoDataSource{{
					TrustBundle: &ateapi.TrustBundleDataSource{
						Names: []string{TrustBundleEgressMITM, TrustBundleSystemRoots},
						Path:  TrustBundleRelPath,
					},
				}},
			},
		},
	}
}

// CanonicalVolumeMounts returns the exact 4 disjoint VolumeMounts in order
// required on every container of a Tekton ActorTemplate.
func CanonicalVolumeMounts() []*ateapi.VolumeMount {
	return []*ateapi.VolumeMount{
		{Name: VolumeNameWorkspace, MountPath: MountPathWorkspace},
		{Name: VolumeNameTekton, MountPath: MountPathTekton},
		{Name: VolumeNameSupervisorBin, MountPath: MountPathSupervisor},
		{Name: VolumeNameAteTrust, MountPath: MountPathAteTrust},
	}
}

// SynthesizeTemplate builds a deterministic, content-addressed ActorTemplate
// for the given steps and sidecars.
func SynthesizeTemplate(ctx context.Context, resolver ImageDigestResolver, params TemplateParams) (*SynthesizedTemplate, error) {
	if params.Atespace == "" {
		return nil, fmt.Errorf("atespace is required")
	}
	if params.SupervisorImage == "" {
		return nil, fmt.Errorf("%s must be configured for Actor workspaces", EnvSupervisorImage)
	}
	if len(params.Steps) == 0 {
		return nil, fmt.Errorf("task must define at least one step")
	}

	supImage, err := resolver.ResolveImageDigest(ctx, params.SupervisorImage)
	if err != nil {
		return nil, fmt.Errorf("resolve supervisor image %q: %w", params.SupervisorImage, err)
	}

	var uniqueStepImages []string
	imageToContainerIdx := make(map[string]int, len(params.Steps))
	stepContainerIndices := make([]int, len(params.Steps))
	stepContainerImages := make([]string, len(params.Steps))

	maxCPU := DefaultCPUMillicores
	maxMem := DefaultMemoryBytes

	for i, step := range params.Steps {
		pinned, resolveErr := resolver.ResolveImageDigest(ctx, step.Image)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve step %q image %q: %w", step.Name, step.Image, resolveErr)
		}
		idx, seen := imageToContainerIdx[pinned]
		if !seen {
			idx = len(uniqueStepImages)
			imageToContainerIdx[pinned] = idx
			uniqueStepImages = append(uniqueStepImages, pinned)
		}
		stepContainerIndices[i] = idx
		stepContainerImages[i] = pinned
		if step.CpuMillicores > maxCPU {
			maxCPU = step.CpuMillicores
		}
		if step.MemoryBytes > maxMem {
			maxMem = step.MemoryBytes
		}
	}

	totalContainers := len(uniqueStepImages) + len(params.Sidecars)
	if totalContainers > 10 {
		return nil, fmt.Errorf("task requires %d distinct containers (max 10 in ActorTemplate)", totalContainers)
	}

	containers := make([]*ateapi.Container, 0, totalContainers)
	for idx, img := range uniqueStepImages {
		ctr := &ateapi.Container{
			Name:         fmt.Sprintf("c%d", idx),
			Image:        img,
			Env:          []*ateapi.EnvVar{{Name: "SSL_CERT_FILE", Value: TrustBundleFullPath}},
			VolumeMounts: CanonicalVolumeMounts(),
		}
		if idx == 0 {
			ctr.Command = []string{
				SupervisorBinaryPath,
				"--listen=" + CoordinatorListenAddr,
				"--role=coordinator",
			}
			ctr.WakeupProbe = &ateapi.WakeupProbe{
				HttpGet: &ateapi.HttpGetAction{
					Path: "/readyz",
					Port: CoordinatorPort,
				},
				TimeoutSeconds: 30,
			}
		} else {
			ctr.Command = []string{
				SupervisorBinaryPath,
				"--listen=" + WorkerLoopbackAddr(idx),
				"--role=worker",
			}
		}
		containers = append(containers, ctr)
	}

	for sIdx, sc := range params.Sidecars {
		pinned, resolveErr := resolver.ResolveImageDigest(ctx, sc.Image)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve sidecar %q image %q: %w", sc.Name, sc.Image, resolveErr)
		}
		if sc.CpuMillicores > maxCPU {
			maxCPU = sc.CpuMillicores
		}
		if sc.MemoryBytes > maxMem {
			maxMem = sc.MemoryBytes
		}
		cmd := append([]string(nil), sc.Command...)
		args := append([]string(nil), sc.Args...)
		if sc.Script != "" {
			cmd = []string{"/bin/sh", "-c", sc.Script}
			args = nil
		}
		envVars := buildContainerEnvVars(sc.Env)
		containers = append(containers, &ateapi.Container{
			Name:         fmt.Sprintf("s%d", sIdx),
			Image:        pinned,
			Command:      cmd,
			Args:         args,
			Env:          envVars,
			VolumeMounts: CanonicalVolumeMounts(),
		})
	}

	var selector *ateapi.Selector
	if len(params.WorkerSelector) > 0 {
		selector = &ateapi.Selector{
			MatchLabels: maps.Clone(params.WorkerSelector),
		}
	}

	tmpl := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: params.Atespace,
		},
		WorkerSelector: selector,
		Containers:     containers,
		Volumes:        CanonicalVolumes(supImage),
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{
				Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED,
			},
		},
		Resources: &ateapi.ResourceRequirements{
			Limits: &ateapi.Resources{
				CpuMillicores: maxCPU,
				MemoryBytes:   maxMem,
			},
		},
	}
	tmpl.Metadata.Name = computeTemplateName(tmpl)

	return &SynthesizedTemplate{
		Template:             tmpl,
		StepContainerIndices: stepContainerIndices,
		StepContainerImages:  stepContainerImages,
	}, nil
}

func buildContainerEnvVars(entries []EnvEntry) []*ateapi.EnvVar {
	byName := map[string]string{
		"SSL_CERT_FILE": TrustBundleFullPath,
	}
	order := []string{"SSL_CERT_FILE"}
	for _, e := range entries {
		if e.Name == "" {
			continue
		}
		if _, exists := byName[e.Name]; !exists {
			order = append(order, e.Name)
		}
		byName[e.Name] = e.Value
	}
	out := make([]*ateapi.EnvVar, 0, len(order))
	for _, k := range order {
		if len(out) >= 32 {
			break
		}
		out = append(out, &ateapi.EnvVar{Name: k, Value: byName[k]})
	}
	return out
}

func computeTemplateName(tmpl *ateapi.ActorTemplate) string {
	type hashPayload struct {
		SupervisorRef  string
		Containers     []*ateapi.Container
		CpuMillicores  int64
		MemoryBytes    int64
		SelectorKeys   []string
		SelectorValues []string
	}
	var selKeys, selVals []string
	if m := tmpl.GetWorkerSelector().GetMatchLabels(); len(m) > 0 {
		selKeys = make([]string, 0, len(m))
		for k := range m {
			selKeys = append(selKeys, k)
		}
		sort.Strings(selKeys)
		selVals = make([]string, len(selKeys))
		for i, k := range selKeys {
			selVals[i] = m[k]
		}
	}
	var supRef string
	for _, v := range tmpl.GetVolumes() {
		if v.GetImage() != nil {
			supRef = v.GetImage().GetReference()
		}
	}
	raw, _ := json.Marshal(hashPayload{
		SupervisorRef:  supRef,
		Containers:     tmpl.GetContainers(),
		CpuMillicores:  tmpl.GetResources().GetLimits().GetCpuMillicores(),
		MemoryBytes:    tmpl.GetResources().GetLimits().GetMemoryBytes(),
		SelectorKeys:   selKeys,
		SelectorValues: selVals,
	})
	sum := sha256.Sum256(raw)
	return "tkn-tmpl-" + hex.EncodeToString(sum[:])[:12]
}
