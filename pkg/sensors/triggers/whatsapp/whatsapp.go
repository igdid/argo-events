/*
Copyright 2026 The Argoproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package whatsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	notifications "github.com/argoproj/notifications-engine/pkg/services"
	"go.uber.org/zap"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	"github.com/argoproj/argo-events/pkg/sensors/triggers"
	"github.com/argoproj/argo-events/pkg/shared/logging"
	sharedutil "github.com/argoproj/argo-events/pkg/shared/util"
)

type WhatsAppTrigger struct {
	// Sensor refer to the sensor object
	Sensor *v1alpha1.Sensor
	// Trigger refers to the trigger resource
	Trigger *v1alpha1.Trigger
	// Logger to log stuff
	Logger *zap.SugaredLogger
	// http client to invoke function.
	httpClient *http.Client
	// whatsappSvc refers to the WhatsApp notification service.
	whatsappSvc notifications.NotificationService
}

// NewWhatsAppTrigger returns a new WhatsApp trigger context
func NewWhatsAppTrigger(sensor *v1alpha1.Sensor, trigger *v1alpha1.Trigger, logger *zap.SugaredLogger, httpClient *http.Client) (*WhatsAppTrigger, error) {
	whatsappTrigger := trigger.Template.WhatsApp
	whatsappToken, err := sharedutil.GetSecretFromVolume(whatsappTrigger.WhatsAppToken)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the whatsapp token, %w", err)
	}

	whatsappSvc := notifications.NewWhatsAppService(notifications.WhatsAppOptions{
		Token:    whatsappToken,
		Username: whatsappTrigger.Sender.Username,
		Icon:     whatsappTrigger.Sender.Icon,
	})

	return &WhatsAppTrigger{
		Sensor:     sensor,
		Trigger:    trigger,
		Logger:     logger.With(logging.LabelTriggerType, v1alpha1.TriggerTypeWhatsApp),
		httpClient: httpClient,
		whatsappSvc:   whatsappSvc,
	}, nil
}

// GetTriggerType returns the type of the trigger
func (t *WhatsAppTrigger) GetTriggerType() v1alpha1.TriggerType {
	return v1alpha1.TriggerTypeWhatsApp
}

func (t *WhatsAppTrigger) FetchResource(ctx context.Context) (interface{}, error) {
	return t.Trigger.Template.WhatsApp, nil
}

func (t *WhatsAppTrigger) ApplyResourceParameters(events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	resourceBytes, err := json.Marshal(resource)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the WhatsApp trigger resource, %w", err)
	}
	parameters := t.Trigger.Template.WhatsApp.Parameters

	if parameters != nil {
		updatedResourceBytes, err := triggers.ApplyParams(resourceBytes, t.Trigger.Template.WhatsApp.Parameters, events)
		if err != nil {
			return nil, err
		}

		var st *v1alpha1.WhatsAppTrigger
		if err := json.Unmarshal(updatedResourceBytes, &st); err != nil {
			return nil, fmt.Errorf("failed to unmarshal the updated WhatsApp trigger resource after applying resource parameters, %w", err)
		}

		return st, nil
	}

	return resource, nil
}

// Execute executes the trigger
func (t *WhatsAppTrigger) Execute(ctx context.Context, events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	t.Logger.Info("executing WhatsAppTrigger")
	_, ok := resource.(*v1alpha1.WhatsAppTrigger)
	if !ok {
		return nil, fmt.Errorf("failed to marshal the WhatsApp trigger resource")
	}

	whatsappTrigger := t.Trigger.Template.WhatsApp

	channel := whatsappTrigger.Channel
	if channel == "" {
		return nil, fmt.Errorf("no whatsapp channel provided")
	}
	channel = strings.TrimPrefix(channel, "#")

	message := whatsappTrigger.Message
	attachments := whatsappTrigger.Attachments
	blocks := whatsappTrigger.Blocks
	if message == "" && attachments == "" && blocks == "" {
		return nil, fmt.Errorf("no text to post: At least one of message/attachments/blocks should be provided")
	}

	t.Logger.Infow("posting to channel...", zap.Any("channelName", channel))

	notification := notifications.Notification{
		Message: message,
		WhatsApp: &notifications.WhatsAppNotification{
			GroupingKey:     whatsappTrigger.Thread.MessageAggregationKey,
			NotifyBroadcast: whatsappTrigger.Thread.BroadcastMessageToChannel,
			Blocks:          blocks,
			Attachments:     attachments,
		},
	}
	destination := notifications.Destination{
		Service:   "whatsapp",
		Recipient: channel,
	}
	err := t.whatsappSvc.Send(notification, destination)
	if err != nil {
		t.Logger.Errorw("unable to post to channel", zap.Any("channelName", channel), zap.Error(err))
		return nil, fmt.Errorf("failed to post to channel %s, %w", channel, err)
	}

	t.Logger.Infow("message successfully sent to channel", zap.Any("message", message), zap.Any("channelName", channel))
	t.Logger.Info("finished executing WhatsAppTrigger")
	return nil, nil
}

// No Policies for WhatsAppTrigger
func (t *WhatsAppTrigger) ApplyPolicy(ctx context.Context, resource interface{}) error {
	return nil
}
