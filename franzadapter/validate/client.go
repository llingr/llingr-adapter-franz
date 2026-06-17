// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

// Package validate provides configuration validation for franz-go clients.
//
// Unlike librdkafka's ConfigMap, franz-go uses functional options (kgo.Opt) that can be
// introspected after client creation via Client.OptValue(). This enables runtime validation
// of critical settings.
package validate

import (
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client validates kgo.Client configuration and helps users avoid misconfiguration.
//
// Call this after creating a kgo.Client, especially when using NewCustom() where the
// application controls all client options.
//
// For New()/NewWithOptions(), critical options are enforced by the adapter, but this
// validation still provides useful warnings about timeout settings.
func Client(client *kgo.Client) {
	ensureAutoCommitDisabled(client)
	ensureBlockRebalanceOnPoll(client)
	logBalancerProtocol(client)
	warnIfReadUncommitted(client)
	warnIfSessionTimeoutTooLow(client)
	warnIfRebalanceTimeoutTooLow(client)
}

// ensureAutoCommitDisabled panics if auto-commit is enabled.
//
// Auto-commit races with explicit offset management, risking message loss. With concurrent
// processing, messages may be in-flight for extended periods. Auto-committing before
// processing completes means crashed applications skip uncommitted messages on restart.
func ensureAutoCommitDisabled(client *kgo.Client) {
	val := client.OptValue(kgo.DisableAutoCommit)
	if val == nil {
		// DisableAutoCommit not set - auto-commit is enabled (default)
		panic("kgo.DisableAutoCommit() is required; auto-commit conflicts with explicit offset " +
			"management and risks message loss")
	}

	disabled, ok := val.(bool)
	if !ok || !disabled {
		panic("kgo.DisableAutoCommit() is required; auto-commit conflicts with explicit offset " +
			"management and risks message loss")
	}
}

// ensureBlockRebalanceOnPoll panics if BlockRebalanceOnPoll is not enabled.
//
// BlockRebalanceOnPoll ensures rebalance callbacks don't fire during message processing.
// Without this, a rebalance could revoke partitions while messages are in-flight, causing
// the adapter to commit offsets for partitions it no longer owns.
//
// The adapter calls AllowRebalance() after each poll cycle to permit rebalances at safe points.
func ensureBlockRebalanceOnPoll(client *kgo.Client) {
	val := client.OptValue(kgo.BlockRebalanceOnPoll)
	if val == nil {
		panic("kgo.BlockRebalanceOnPoll() is required; without it, rebalances can occur during " +
			"message processing, risking offset commits for revoked partitions")
	}

	enabled, ok := val.(bool)
	if !ok || !enabled {
		panic("kgo.BlockRebalanceOnPoll() is required; without it, rebalances can occur during " +
			"message processing, risking offset commits for revoked partitions")
	}
}

// Rebalance callbacks (OnPartitionsAssigned/Revoked/Lost) are not validated:
// franz-go wraps them non-nil at client creation, so OptValue always reports
// them present. Adapter.RequiredOpts carries them instead.

// logBalancerProtocol logs the configured rebalancing protocol for operational visibility.
//
// Franz-go supports both cooperative and eager rebalancing strategies:
//
//   - Cooperative (CooperativeStickyBalancer): Incremental rebalancing where only affected
//     partitions are revoked/assigned. Other partitions continue processing uninterrupted.
//     Minimises disruption during consumer group membership changes.
//
//   - Eager (RangeBalancer, RoundRobinBalancer, StickyBalancer): Traditional rebalancing
//     where all partitions are revoked before reassignment. Simpler protocol with brief
//     processing pause during rebalances.
//
// Both protocols work correctly with the adapter's drain coordination. The choice depends
// on your operational requirements - cooperative for minimal disruption, eager for simpler
// debugging and deterministic partition assignment.
//
// This mirrors the protocol logging in llingr-adapter-kafka for operational consistency
// across adapters.
func logBalancerProtocol(client *kgo.Client) {
	val := client.OptValue(kgo.Balancers)
	if val == nil {
		// Balancers not explicitly configured - franz-go requires this for consumer groups,
		// so this indicates either a producer-only client or misconfiguration
		return
	}

	balancers, ok := val.([]kgo.GroupBalancer)
	if !ok || len(balancers) == 0 {
		return
	}

	// When multiple balancers are configured (e.g., via CompatibilityBalancers()),
	// skip logging here - the loggingBalancer wrapper will log the actual selected
	// protocol during group negotiation, with appropriate warnings for fallbacks.
	if len(balancers) > 1 {
		return
	}

	// Single balancer: log immediately since there's no negotiation fallback
	primary := balancers[0]
	protocolName := primary.ProtocolName()

	if primary.IsCooperative() {
		log.Printf("franzadapter: using %s balancer (cooperative); "+
			"only affected partitions revoked during rebalance", protocolName)
	} else {
		log.Printf("franzadapter: using %s balancer (eager); "+
			"all partitions revoked during rebalance", protocolName)
	}
}

// warnIfReadUncommitted warns if FetchIsolationLevel is ReadUncommitted.
//
// Unlike librdkafka (which defaults to read_committed), franz-go defaults to ReadUncommitted.
// This means aborted transaction messages are visible to consumers, breaking exactly-once
// guarantees when used with transactional producers.
//
// If your producers don't use transactions, ReadUncommitted is safe. Otherwise, configure
// kgo.FetchIsolationLevel(kgo.ReadCommitted()) for transactional safety.
func warnIfReadUncommitted(client *kgo.Client) {
	val := client.OptValue(kgo.FetchIsolationLevel)
	if val == nil {
		// Not set - using default (ReadUncommitted)
		log.Printf("franzadapter: FetchIsolationLevel defaults to ReadUncommitted; " +
			"if using transactional producers, set kgo.FetchIsolationLevel(kgo.ReadCommitted()) " +
			"to avoid reading aborted transaction messages")
		return
	}

	// OptValue returns int8 for FetchIsolationLevel, not kgo.IsolationLevel
	level, ok := val.(int8)
	if !ok {
		return
	}

	// ReadUncommitted is 0, ReadCommitted is 1
	if level == 0 {
		log.Printf("franzadapter: FetchIsolationLevel is ReadUncommitted; " +
			"if using transactional producers, set kgo.FetchIsolationLevel(kgo.ReadCommitted()) " +
			"to avoid reading aborted transaction messages")
	}
}

// warnIfSessionTimeoutTooLow warns if SessionTimeout is below the typical drain timeout.
//
// During graceful shutdown or rebalancing, in-flight messages must complete processing and
// commit before the consumer leaves the group. If SessionTimeout is shorter than the drain
// period, the broker may eject the consumer before commits complete, causing duplicate
// processing when partitions are reassigned.
//
// The default drain timeout is 20 seconds. Session timeout should comfortably exceed this
// to allow for drain completion plus commit round-trip time.
func warnIfSessionTimeoutTooLow(client *kgo.Client) {
	const minRecommended = 25 * time.Second // 20s drain + 5s buffer

	val := client.OptValue(kgo.SessionTimeout)
	if val == nil {
		return // using default (45s)
	}

	timeout, ok := val.(time.Duration)
	if !ok {
		return
	}

	if timeout < minRecommended {
		log.Printf("franzadapter: SessionTimeout=%v is below recommended minimum of %v; "+
			"consumer may be ejected from group before in-flight work completes during "+
			"graceful shutdown, causing duplicate processing when partitions are reassigned",
			timeout, minRecommended)
	}
}

// warnIfRebalanceTimeoutTooLow warns if RebalanceTimeout is below 30 seconds.
//
// RebalanceTimeout (equivalent to Kafka's max.poll.interval.ms) is the maximum time between
// poll calls before the consumer is considered failed. Container orchestration platforms like
// Kubernetes use a termination grace period (default 30s) to allow graceful shutdown.
//
// If RebalanceTimeout is lower than this grace period, the consumer may be ejected from the
// group before the application finishes draining in-flight work during shutdown.
func warnIfRebalanceTimeoutTooLow(client *kgo.Client) {
	const minRecommended = 30 * time.Second

	val := client.OptValue(kgo.RebalanceTimeout)
	if val == nil {
		return // using default (60s)
	}

	timeout, ok := val.(time.Duration)
	if !ok {
		return
	}

	if timeout < minRecommended {
		log.Printf("franzadapter: RebalanceTimeout=%v is below recommended minimum of %v; "+
			"container orchestrators typically use 30s termination grace periods - "+
			"a lower timeout risks group ejection before graceful shutdown completes",
			timeout, minRecommended)
	}
}
