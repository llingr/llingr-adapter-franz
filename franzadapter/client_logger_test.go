// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The bridge must map each franz-go level onto the matching consumer-logger
// method, so client diagnostics carry their real severity.
func TestClientLogBridge_LevelMapping(t *testing.T) {
	logger := &capturingLogger{}
	bridge := newClientLogBridge(kgo.LogLevelDebug)
	bridge.attach(context.Background(), logger)

	bridge.Log(kgo.LogLevelError, "boom")
	bridge.Log(kgo.LogLevelWarn, "wobble")
	bridge.Log(kgo.LogLevelInfo, "joined")
	bridge.Log(kgo.LogLevelDebug, "detail")

	if !logger.errorCalled || !logger.warnCalled || !logger.infoCalled || !logger.debugCalled {
		t.Fatalf("expected all four levels forwarded: error=%v warn=%v info=%v debug=%v",
			logger.errorCalled, logger.warnCalled, logger.infoCalled, logger.debugCalled)
	}
}

// Level() is what franz-go consults to skip message assembly; it must report
// the configured verbosity.
func TestClientLogBridge_Level(t *testing.T) {
	if got := newClientLogBridge(kgo.LogLevelWarn).Level(); got != kgo.LogLevelWarn {
		t.Fatalf("Level() = %v, want %v", got, kgo.LogLevelWarn)
	}
}

// Lines logged before the consumer's logger exists (client construction) are
// dropped, never a panic.
func TestClientLogBridge_DropsBeforeAttach(t *testing.T) {
	bridge := newClientLogBridge(kgo.LogLevelInfo)
	bridge.Log(kgo.LogLevelInfo, "pre-attach") // must not panic

	logger := &capturingLogger{}
	bridge.attach(context.Background(), logger)
	bridge.Log(kgo.LogLevelInfo, "post-attach")

	if logger.lastInfoMsg != "franz-go: post-attach" {
		t.Fatalf("post-attach line = %q", logger.lastInfoMsg)
	}
}

func TestFormatClientLog(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		keyvals []any
		want    string
	}{
		{
			name: "no keyvals",
			msg:  "immediate metadata update triggered",
			want: "franz-go: immediate metadata update triggered",
		},
		{
			name:    "pairs",
			msg:     "leaving group",
			keyvals: []any{"group", "llingr-chaos-group", "member_id", "abc-123"},
			want:    "franz-go: leaving group; group=llingr-chaos-group member_id=abc-123",
		},
		{
			name:    "trailing key without value",
			msg:     "odd",
			keyvals: []any{"lonely"},
			want:    "franz-go: odd; lonely",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatClientLog(tt.msg, tt.keyvals); got != tt.want {
				t.Errorf("formatClientLog = %q, want %q", got, tt.want)
			}
		})
	}
}

// The default adapter options bridge at Info: the group lifecycle (joins,
// leaves and their failures, rebalance rounds) is visible out of the box.
func TestClientLogLevel_DefaultAndOverride(t *testing.T) {
	if got := defaultAdapterOptions().clientLogLevel; got != kgo.LogLevelInfo {
		t.Fatalf("default client log level = %v, want %v", got, kgo.LogLevelInfo)
	}
	opts := processAdapterOptions(WithClientLogLevel(kgo.LogLevelNone))
	if opts.clientLogLevel != kgo.LogLevelNone {
		t.Fatalf("override client log level = %v, want %v", opts.clientLogLevel, kgo.LogLevelNone)
	}
}
