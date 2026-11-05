package service

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/settings"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
)

// applyRunSettings fills fields the caller left unset with values resolved from
// settings. An explicit value in the request always wins, so this only ever fills
// gaps, and a setting that is INHERIT or UNSET contributes nothing.
//
// It returns an error, before changing spec, if a label or annotation it would add is
// invalid. Only entries from settings are checked: the request's own entries keep
// their existing behavior. Settings are validated on write, so this catches rows
// stored before that check existed.
func applyRunSettings(spec *task.RunSpec, resolved *settings.Settings) error {
	if spec == nil {
		return nil
	}

	// Map settings merge per key: the request's entry wins on a conflict, and the
	// settings only add keys the request does not set.
	labels := entriesToAdd(resolved.GetLabels(), spec.GetLabels().GetValues())
	if err := validateLabels(labels); err != nil {
		return fromSettings(err)
	}
	annotations := entriesToAdd(resolved.GetAnnotations(), spec.GetAnnotations().GetValues())
	if err := validateAnnotations(annotations); err != nil {
		return fromSettings(err)
	}

	if spec.GetQueue() == "" && resolved.GetRun().GetDefaultQueue().GetState() ==
		settings.SettingState_SETTING_STATE_VALUE {
		spec.Queue = resolved.GetRun().GetDefaultQueue().GetStringValue()
	}

	// The proto defines 0 as unset for this field, so there is no explicit request for
	// "unlimited" to override. The settings validator bounds the value to 0 or
	// [2, MaxUint32], so narrowing to uint32 cannot overflow.
	if concurrency := resolved.GetRun().GetMaxActionConcurrency(); spec.GetMaxActionConcurrency() == 0 &&
		concurrency.GetState() == settings.SettingState_SETTING_STATE_VALUE {
		spec.MaxActionConcurrency = uint32(concurrency.GetIntValue())
	}

	// Projected onto tasks that name no template of their own, at action creation.
	if podTemplate := resolved.GetPodTemplateName(); spec.GetDefaultSettings().GetPodTemplateName() == "" &&
		podTemplate.GetState() == settings.SettingState_SETTING_STATE_VALUE {
		if spec.DefaultSettings == nil {
			spec.DefaultSettings = &task.DefaultSettings{}
		}
		spec.DefaultSettings.PodTemplateName = podTemplate.GetStringValue()
	}

	// Base for run metadata; also raw outputs when raw_data_prefix is empty.
	if baseDir := resolved.GetRun().GetRunBaseDir(); spec.GetRunBaseDir() == "" &&
		baseDir.GetState() == settings.SettingState_SETTING_STATE_VALUE {
		spec.RunBaseDir = baseDir.GetStringValue()
	}

	// RawDataStorage carries only this prefix, so replacing the message loses nothing.
	if rawDataPath := resolved.GetStorage().GetRawDataPath(); spec.GetRawDataStorage().GetRawDataPrefix() == "" &&
		rawDataPath.GetState() == settings.SettingState_SETTING_STATE_VALUE {
		spec.RawDataStorage = &task.RawDataStorage{RawDataPrefix: rawDataPath.GetStringValue()}
	}

	if len(labels) > 0 {
		if spec.Labels == nil {
			spec.Labels = &task.Labels{}
		}
		spec.Labels.Values = withEntries(spec.Labels.Values, labels)
	}

	if len(annotations) > 0 {
		if spec.Annotations == nil {
			spec.Annotations = &task.Annotations{}
		}
		spec.Annotations.Values = withEntries(spec.Annotations.Values, annotations)
	}

	// Envs is a list, so the request's entries keep their order and settings entries
	// are appended in key order, keeping the stored spec deterministic.
	present := make(map[string]string, len(spec.GetEnvs().GetValues()))
	for _, kv := range spec.GetEnvs().GetValues() {
		present[kv.GetKey()] = kv.GetValue()
	}
	if envs := entriesToAdd(resolved.GetEnvironmentVariables(), present); len(envs) > 0 {
		if spec.Envs == nil {
			spec.Envs = &task.Envs{}
		}
		for _, k := range slices.Sorted(maps.Keys(envs)) {
			spec.Envs.Values = append(spec.Envs.Values, &core.KeyValuePair{Key: k, Value: envs[k]})
		}
	}
	return nil
}

// valueEntries returns the entries of a map setting, or nil unless it is in VALUE
// state. Resolution has already merged the scopes, so UNSET and INHERIT add nothing.
func valueEntries(s *settings.StringMapSetting) map[string]string {
	if s.GetState() != settings.SettingState_SETTING_STATE_VALUE {
		return nil
	}
	return s.GetMapValue().GetEntries()
}

// entriesToAdd returns the VALUE entries of a map setting whose keys present does not
// already have, or nil if there are none.
func entriesToAdd(s *settings.StringMapSetting, present map[string]string) map[string]string {
	var out map[string]string
	for k, v := range valueEntries(s) {
		if _, ok := present[k]; ok {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[k] = v
	}
	return out
}

// withEntries copies src into dst, allocating dst if needed.
func withEntries(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string, len(src))
	}
	maps.Copy(dst, src)
	return dst
}

// fromSettings marks a validation error as coming from settings, since the caller
// may not have set any label or annotation themselves.
func fromSettings(err error) error {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return err
	}
	return connect.NewError(connectErr.Code(), fmt.Errorf("from settings: %s", connectErr.Message()))
}
