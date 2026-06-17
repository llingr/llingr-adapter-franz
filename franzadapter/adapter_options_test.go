// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"testing"
	"time"
)

func TestProcessAdapterOptions_Defaults(t *testing.T) {
	o := processAdapterOptions()
	if o.pollErrorBailAfter != 10*time.Minute || o.pollErrorBackoff != 25*time.Millisecond ||
		o.pollErrorLogInterval != time.Second || o.bailTerminate == nil {
		t.Errorf("processAdapterOptions() not fully defaulted: %+v", o)
	}
}

func TestProcessOptions_BailAfter(t *testing.T) {
	if got := processAdapterOptions().pollErrorBailAfter; got != 10*time.Minute {
		t.Errorf("no option: bailAfter = %s, want default 10m", got)
	}
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero disables", 0, 0},
		{"negative disables", -1, 0},
		{"below floor clamps to 1m", 5 * time.Second, time.Minute},
		{"at floor passes through", time.Minute, time.Minute},
		{"in range passes through", 30 * time.Minute, 30 * time.Minute},
		{"at ceiling passes through", time.Hour, time.Hour},
		{"above ceiling clamps to 1h", 3 * time.Hour, time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := processAdapterOptions(WithPollErrorBailAfter(tc.in)).pollErrorBailAfter
			if got != tc.want {
				t.Errorf("WithPollErrorBailAfter(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestProcessOptions_Backoff(t *testing.T) {
	if got := processAdapterOptions().pollErrorBackoff; got != 25*time.Millisecond {
		t.Errorf("no option: backoff = %s, want default 25ms", got)
	}
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero disables", 0, 0},
		{"negative disables", -1, 0},
		{"small value passes through", time.Millisecond, time.Millisecond},
		{"in range passes through", 100 * time.Millisecond, 100 * time.Millisecond},
		{"at ceiling passes through", 5 * time.Second, 5 * time.Second},
		{"above ceiling clamps to 5s", 30 * time.Second, 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := processAdapterOptions(WithPollErrorBackoff(tc.in)).pollErrorBackoff
			if got != tc.want {
				t.Errorf("WithPollErrorBackoff(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestProcessOptions_LogIntervalAndTerminate(t *testing.T) {
	t.Run("non-positive log interval falls back to default", func(t *testing.T) {
		if got := processAdapterOptions(WithPollErrorLogInterval(0)).pollErrorLogInterval; got != time.Second {
			t.Errorf("logInterval(0) = %s, want 1s", got)
		}
		if got := processAdapterOptions(WithPollErrorLogInterval(-1)).pollErrorLogInterval; got != time.Second {
			t.Errorf("logInterval(-1) = %s, want 1s", got)
		}
	})
	t.Run("explicit log interval passes through", func(t *testing.T) {
		if got := processAdapterOptions(WithPollErrorLogInterval(5 * time.Second)).pollErrorLogInterval; got != 5*time.Second {
			t.Errorf("logInterval(5s) = %s, want 5s", got)
		}
	})
	t.Run("nil terminate defaults; supplied terminate retained", func(t *testing.T) {
		if processAdapterOptions(WithBailTerminate(nil)).bailTerminate == nil {
			t.Error("nil BailTerminate should default to a non-nil action")
		}
		called := false
		processAdapterOptions(WithBailTerminate(func() {
			called = true
		})).bailTerminate()
		if !called {
			t.Error("supplied BailTerminate should be retained")
		}
	})
}
