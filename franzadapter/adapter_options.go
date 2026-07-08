// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"os"
	"time"
)

// adapterOptions is the resolved poll-error handling configuration, set through
// the AdapterOption helpers and processAdapterOptions.
type adapterOptions struct {
	// pollErrorLogInterval throttles repeats of the same partition error; a new
	// or changed error always logs immediately.
	pollErrorLogInterval time.Duration

	// pollErrorBailAfter is how long a partition may fail continuously before
	// the consumer stops itself (Shutdown, then process termination), turning a
	// stuck partition into a reschedulable event rather than silent lag.
	pollErrorBailAfter time.Duration

	// pollErrorBackoff is the pause after a broker error with no record, so the
	// loop does not spin when kgo returns a buffered error immediately. The
	// pause is context-aware.
	pollErrorBackoff time.Duration

	// bailTerminate ends the process after a bail's Shutdown so an orchestrator
	// reschedules the pod instead of leaving a zombie replica. Nil uses the
	// default (os.Exit(1)).
	bailTerminate func()

	// assignmentOffsetLookup queries the broker for the group's committed offsets
	// when partitions are assigned (one coordinator round trip per assign event,
	// batched across partitions), so the consumer starts from a real baseline
	// instead of the -1 unknown sentinel. Enabled by default.
	assignmentOffsetLookup bool
}

// Poll-error handling defaults and bounds.
const (
	defaultPollErrorLogInterval = time.Second

	defaultPollErrorBailAfter = 10 * time.Minute
	minPollErrorBailAfter     = time.Minute
	maxPollErrorBailAfter     = time.Hour

	defaultPollErrorBackoff = 25 * time.Millisecond
	maxPollErrorBackoff     = 5 * time.Second

	// assignmentOffsetLookupTimeout bounds the committed-offset query at assign
	// time; on timeout the assign proceeds with unknown (-1) baselines. The
	// coordinator can be mid-move during exactly the churn that produces
	// assigns, so the lookup must never stall the poll loop for long.
	assignmentOffsetLookupTimeout = 5 * time.Second
)

// defaultBailTerminate ends the process after a bail.
var defaultBailTerminate = func() {
	os.Exit(1)
}

// AdapterOption configures poll-error handling; the With* helpers return them.
type AdapterOption func(*adapterOptions)

// WithPollErrorLogInterval sets the minimum interval between repeat logs of the
// same partition error.
func WithPollErrorLogInterval(d time.Duration) AdapterOption {
	return func(o *adapterOptions) {
		o.pollErrorLogInterval = d
	}
}

// WithPollErrorBailAfter sets how long a partition may fail continuously before
// the consumer stops itself. 0 or negative disables the bail.
func WithPollErrorBailAfter(d time.Duration) AdapterOption {
	return func(o *adapterOptions) {
		o.pollErrorBailAfter = d
	}
}

// WithPollErrorBackoff sets the pause after a broker error with no record. 0 or
// negative disables the pause.
func WithPollErrorBackoff(d time.Duration) AdapterOption {
	return func(o *adapterOptions) {
		o.pollErrorBackoff = d
	}
}

// WithBailTerminate sets the action that ends the process after a bail's
// Shutdown (nil falls back to the default).
func WithBailTerminate(fn func()) AdapterOption {
	return func(o *adapterOptions) {
		o.bailTerminate = fn
	}
}

// WithAssignmentOffsetLookup enables or disables the committed-offset query on
// partition assignment (default: enabled). When enabled, the adapter spends
// one coordinator round trip per assign event (batched across partitions) to
// report each partition's real committed offset; when disabled, or when the
// query fails, the committed offset is reported as -1 (unknown). Disabling
// saves the round trip at the cost of a rare extra at-least-once redelivery
// window after abnormal rebalances.
func WithAssignmentOffsetLookup(enabled bool) AdapterOption {
	return func(o *adapterOptions) {
		o.assignmentOffsetLookup = enabled
	}
}

// defaultAdapterOptions is the starting point: every field at its default.
func defaultAdapterOptions() adapterOptions {
	return adapterOptions{
		pollErrorLogInterval:   defaultPollErrorLogInterval,
		pollErrorBailAfter:     defaultPollErrorBailAfter,
		pollErrorBackoff:       defaultPollErrorBackoff,
		bailTerminate:          defaultBailTerminate,
		assignmentOffsetLookup: true,
	}
}

// processAdapterOptions folds the options over the defaults, then validates. The
// one place defaults, overrides, and validation meet.
func processAdapterOptions(options ...AdapterOption) adapterOptions {
	o := defaultAdapterOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&o)
		}
	}
	o.validate()
	return o
}

// validate normalises an adapterOptions in place: non-positive log interval and
// nil terminate fall back to defaults; the switchable durations disable on 0 or
// negative and otherwise clamp to bounds.
func (o *adapterOptions) validate() {
	if o.pollErrorLogInterval <= 0 {
		o.pollErrorLogInterval = defaultPollErrorLogInterval
	}
	o.pollErrorBailAfter = clampBailAfter(o.pollErrorBailAfter)
	o.pollErrorBackoff = clampBackoff(o.pollErrorBackoff)
	if o.bailTerminate == nil {
		o.bailTerminate = defaultBailTerminate
	}
}

// clampBailAfter: 0 or negative disables; otherwise clamp to [min, max].
func clampBailAfter(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return 0 // disabled
	case d < minPollErrorBailAfter:
		return minPollErrorBailAfter
	case d > maxPollErrorBailAfter:
		return maxPollErrorBailAfter
	default:
		return d
	}
}

// clampBackoff: 0 or negative disables; otherwise clamp to (0, max].
func clampBackoff(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return 0 // disabled
	case d > maxPollErrorBackoff:
		return maxPollErrorBackoff
	default:
		return d
	}
}
