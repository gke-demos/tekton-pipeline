package ateactor

import (
	"context"
	"fmt"
	"maps"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// EnsureActorParams describes the desired running Actor state for a TaskRun.
type EnsureActorParams struct {
	Atespace       string
	ActorName      string
	Template       *ateapi.ActorTemplate
	SourceTag      string
	WorkerSelector map[string]string
	Namespace      string
	EgressSecret   string
	EgressHosts    []string
}

// EnsureAtespace idempotently ensures the global-scoped Atespace exists.
func EnsureAtespace(ctx context.Context, ctrl ateapi.ControlClient, atespace string) error {
	_, err := ctrl.GetAtespace(ctx, &ateapi.GetAtespaceRequest{
		Atespace: &ateapi.ObjectRef{Name: atespace},
	})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("get atespace %q: %w", atespace, err)
	}
	_, err = ctrl.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
		Atespace: &ateapi.Atespace{
			Metadata: &ateapi.ResourceMetadata{Name: atespace},
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace %q: %w", atespace, err)
	}
	return nil
}

// EnsureActorTemplate ensures the Atespace and content-addressed ActorTemplate
// exist and returns the stored ActorTemplate (including server-assigned UID).
func EnsureActorTemplate(ctx context.Context, ctrl ateapi.ControlClient, tmpl *ateapi.ActorTemplate) (*ateapi.ActorTemplate, error) {
	atespace := tmpl.GetMetadata().GetAtespace()
	name := tmpl.GetMetadata().GetName()
	if err := EnsureAtespace(ctx, ctrl, atespace); err != nil {
		return nil, err
	}

	existing, err := ctrl.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
		ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: name},
	})
	if err == nil {
		return existing, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, fmt.Errorf("get actor template %s/%s: %w", atespace, name, err)
	}

	created, err := ctrl.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: proto.CloneOf(tmpl),
	})
	if err == nil {
		return created, nil
	}
	if status.Code(err) == codes.AlreadyExists {
		return ctrl.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
			ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: name},
		})
	}
	return nil, fmt.Errorf("create actor template %s/%s: %w", atespace, name, err)
}

// EnsureActorReady ensures the Actor exists (creating from SourceTag if needed),
// repoints its ActorTemplate via SuspendActor -> UpdateActor when the toolchain
// changed, ensures per-Actor EgressPolicy when EgressSecret is set, and resumes
// the Actor to ACTOR_STATE_RUNNING.
func EnsureActorReady(ctx context.Context, ctrl ateapi.ControlClient, params EnsureActorParams) (*ateapi.Actor, error) {
	if params.Template == nil {
		return nil, fmt.Errorf("Template is required")
	}
	atespace := params.Atespace
	actorName := params.ActorName
	targetTmplRef := &ateapi.ObjectRef{
		Atespace: atespace,
		Name:     params.Template.GetMetadata().GetName(),
	}

	actor, err := ctrl.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if status.Code(err) == codes.NotFound {
		actor, err = createActorWithOptionalTag(ctx, ctrl, params, targetTmplRef)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("get actor %s/%s: %w", atespace, actorName, err)
	}

	if actor.GetActorTemplate().GetName() != targetTmplRef.GetName() {
		if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
			suspResp, suspErr := ctrl.SuspendActor(ctx, &ateapi.SuspendActorRequest{
				Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
				Options: &ateapi.SuspendOptions{
					Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
				},
			})
			if suspErr != nil {
				return nil, fmt.Errorf("suspend actor %s/%s before template update: %w", atespace, actorName, suspErr)
			}
			actor = suspResp.GetActor()
		}
		actor, err = repointActorTemplate(ctx, ctrl, actor, targetTmplRef)
		if err != nil {
			return nil, err
		}
	}

	if params.EgressSecret != "" {
		if err := EnsureActorEgressPolicy(ctx, ctrl, atespace, actorName, params.Namespace, params.EgressSecret, params.EgressHosts); err != nil {
			return nil, err
		}
	}

	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		resResp, resErr := ctrl.ResumeActor(ctx, &ateapi.ResumeActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
		})
		if resErr != nil {
			return nil, fmt.Errorf("resume actor %s/%s: %w", atespace, actorName, resErr)
		}
		actor = resResp.GetActor()
	}

	return actor, nil
}

func createActorWithOptionalTag(
	ctx context.Context,
	ctrl ateapi.ControlClient,
	params EnsureActorParams,
	targetTmplRef *ateapi.ObjectRef,
) (*ateapi.Actor, error) {
	atespace := params.Atespace
	actorName := params.ActorName
	initialTmplRef := targetTmplRef

	var sourceTagRef *ateapi.ObjectRef
	if params.SourceTag != "" {
		sourceTagRef = &ateapi.ObjectRef{
			Atespace: atespace,
			Name:     params.SourceTag,
		}
		// Substrate CreateActor requires actor_template UID to match the Tag's
		// actor_template_uid at creation time; if the downstream task uses a
		// different ActorTemplate, create under the Tag's template first and
		// immediately UpdateActor while SUSPENDED before ResumeActor.
		tag, err := ctrl.GetTag(ctx, &ateapi.GetTagRequest{Tag: sourceTagRef})
		if err != nil {
			return nil, fmt.Errorf("get source tag %s/%s: %w", atespace, params.SourceTag, err)
		}
		tagTmplUID := tag.GetStatus().GetActorTemplateUid()
		if tagTmplUID != "" && tagTmplUID != params.Template.GetMetadata().GetUid() {
			seedRef, findErr := findTemplateByUID(ctx, ctrl, atespace, tagTmplUID)
			if findErr != nil {
				return nil, findErr
			}
			initialTmplRef = seedRef
		}
	}

	var selector *ateapi.Selector
	if len(params.WorkerSelector) > 0 {
		selector = &ateapi.Selector{
			MatchLabels: maps.Clone(params.WorkerSelector),
		}
	}

	created, err := ctrl.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: atespace,
				Name:     actorName,
			},
			ActorTemplate:  initialTmplRef,
			WorkerSelector: selector,
			SourceTag:      sourceTagRef,
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return ctrl.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
		})
	}
	if err != nil {
		return nil, fmt.Errorf("create actor %s/%s: %w", atespace, actorName, err)
	}
	return created, nil
}

func findTemplateByUID(ctx context.Context, ctrl ateapi.ControlClient, atespace, uid string) (*ateapi.ObjectRef, error) {
	pageToken := ""
	for {
		resp, err := ctrl.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{
			Atespace: atespace,
			Options:  &ateapi.ListOptions{PageSize: 200, PageToken: pageToken},
		})
		if err != nil {
			return nil, fmt.Errorf("list actor templates in %q: %w", atespace, err)
		}
		for _, t := range resp.GetActorTemplates() {
			if t.GetMetadata().GetUid() == uid {
				return &ateapi.ObjectRef{
					Atespace: atespace,
					Name:     t.GetMetadata().GetName(),
				}, nil
			}
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	return nil, fmt.Errorf("source tag ActorTemplate UID %q not found in atespace %q", uid, atespace)
}

func repointActorTemplate(
	ctx context.Context,
	ctrl ateapi.ControlClient,
	actor *ateapi.Actor,
	targetTmplRef *ateapi.ObjectRef,
) (*ateapi.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	actorName := actor.GetMetadata().GetName()

	for attempt := 0; attempt < 2; attempt++ {
		toSend := proto.CloneOf(actor)
		toSend.ActorTemplate = proto.CloneOf(targetTmplRef)
		toSend.Status = nil
		toSend.Metadata = &ateapi.ResourceMetadata{
			Atespace: atespace,
			Name:     actorName,
			Uid:      actor.GetMetadata().GetUid(),
			Version:  actor.GetMetadata().GetVersion(),
		}

		updated, err := ctrl.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: toSend})
		if err == nil {
			return updated, nil
		}
		if status.Code(err) == codes.Aborted && attempt == 0 {
			fresh, getErr := ctrl.GetActor(ctx, &ateapi.GetActorRequest{
				Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
			})
			if getErr != nil {
				return nil, fmt.Errorf("refresh actor %s/%s after conflict: %w", atespace, actorName, getErr)
			}
			actor = fresh
			continue
		}
		return nil, fmt.Errorf("update actor %s/%s template to %s: %w", atespace, actorName, targetTmplRef.GetName(), err)
	}
	return actor, nil
}

// SuspendActorSync checkpoints a running Actor synchronously so its durable
// snapshot is immediately ready in object storage for UpdateActor, cross-node
// ResumeActor, or CreateTag.
func SuspendActorSync(ctx context.Context, ctrl ateapi.ControlClient, atespace, actorName string) error {
	_, err := ctrl.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
		Options: &ateapi.SuspendOptions{
			Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
		},
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("suspend actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// EnsureAtespaceTag idempotently checkpoints sourceActorName synchronously and
// creates an atespace-scoped Snapshot Tag pointing to its latest durable snapshot.
func EnsureAtespaceTag(
	ctx context.Context,
	ctrl ateapi.ControlClient,
	atespace, tagName, sourceActorName string,
) (*ateapi.Tag, error) {
	existing, err := ctrl.GetTag(ctx, &ateapi.GetTagRequest{
		Tag: &ateapi.ObjectRef{Atespace: atespace, Name: tagName},
	})
	if err == nil {
		return existing, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, fmt.Errorf("get tag %s/%s: %w", atespace, tagName, err)
	}
	if err := SuspendActorSync(ctx, ctrl, atespace, sourceActorName); err != nil {
		return nil, err
	}
	created, err := ctrl.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: atespace,
				Name:     tagName,
			},
			Scope: ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{
				Atespace: atespace,
				Name:     sourceActorName,
			},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return ctrl.GetTag(ctx, &ateapi.GetTagRequest{
			Tag: &ateapi.ObjectRef{Atespace: atespace, Name: tagName},
		})
	}
	if err != nil {
		return nil, fmt.Errorf("create tag %s/%s from actor %s: %w", atespace, tagName, sourceActorName, err)
	}
	return created, nil
}

// DeleteActorIgnoreNotFound deletes an Actor if it still exists.
func DeleteActorIgnoreNotFound(ctx context.Context, ctrl ateapi.ControlClient, atespace, actorName string) error {
	if atespace == "" || actorName == "" {
		return nil
	}
	_, err := ctrl.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}
