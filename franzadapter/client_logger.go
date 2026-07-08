// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// clientLogBridge adapts the consumer's nexus.Logger to franz-go's Logger
// interface, so the client's internal diagnostics (group joins and leaves,
// rebalance rounds, heartbeat and commit errors) land in the same stream as
// the adapter's own lines. Without it those events are invisible: franz-go
// swallows, for example, a failed LeaveGroup request silently, and the first
// visible symptom is the coordinator expiring the member a session timeout
// later.
//
// The kgo client is created before the consumer (and therefore its logger)
// exists, so the target is attached later; lines emitted before attachment
// are dropped (client construction only). The atomic makes the late attach
// safe against the client goroutines already logging.
type clientLogBridge struct {
	level  kgo.LogLevel // fixed at construction: kgo requires Level() to be concurrency-safe
	target atomic.Value // holds clientLogTarget
}

type clientLogTarget struct {
	ctx    context.Context
	logger nexus.Logger
}

func newClientLogBridge(level kgo.LogLevel) *clientLogBridge {
	return &clientLogBridge{level: level}
}

// attach sets the destination logger. Called once the consumer is built.
func (b *clientLogBridge) attach(ctx context.Context, logger nexus.Logger) {
	if logger == nil {
		return
	}
	b.target.Store(clientLogTarget{ctx: ctx, logger: logger})
}

// Level implements kgo.Logger: the client skips assembling messages above
// this level entirely.
func (b *clientLogBridge) Level() kgo.LogLevel {
	return b.level
}

// Log implements kgo.Logger.
func (b *clientLogBridge) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	target, ok := b.target.Load().(clientLogTarget)
	if !ok {
		return
	}
	line := formatClientLog(msg, keyvals)
	switch level {
	case kgo.LogLevelError:
		target.logger.Error(target.ctx, line)
	case kgo.LogLevelWarn:
		target.logger.Warn(target.ctx, line)
	case kgo.LogLevelInfo:
		target.logger.Info(target.ctx, line)
	default: // kgo.LogLevelDebug (and anything unexpected stays low-severity)
		target.logger.Debug(target.ctx, line)
	}
}

// formatClientLog renders a kgo message and its key-value pairs as one line:
// "franz-go: joined group; group=g member_id=m". The prefix marks the origin
// so client internals are distinguishable from adapter lines. A trailing key
// without a value is rendered alone.
func formatClientLog(msg string, keyvals []any) string {
	var b strings.Builder
	b.WriteString("franz-go: ")
	b.WriteString(msg)
	for i := 0; i < len(keyvals); i += 2 {
		if i == 0 {
			b.WriteString(";")
		}
		if i+1 < len(keyvals) {
			fmt.Fprintf(&b, " %v=%v", keyvals[i], keyvals[i+1])
		} else {
			fmt.Fprintf(&b, " %v", keyvals[i])
		}
	}
	return b.String()
}
