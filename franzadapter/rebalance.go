// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"fmt"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OnAssigned should be passed to kgo.OnPartitionsAssigned when creating the client.
func (a *Adapter) OnAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	a.mu.Lock()
	for topic, partitions := range assigned {
		a.pendingAssigned[topic] = append(a.pendingAssigned[topic], partitions...)
	}
	a.mu.Unlock()
}

// OnRevoked should be passed to kgo.OnPartitionsRevoked when creating the client.
//
// The revoked partitions' in-flight work is drained and their offsets committed
// HERE, synchronously, before this callback returns. franz-go does not proceed with
// the group's reassignment until OnPartitionsRevoked returns, so committing inside it
// is the only point at which we can guarantee the commit lands before another member
// takes the partition. Draining/committing was previously deferred to the poll loop
// (processPendingRebalances); but AllowRebalance is non-blocking, so franz-go's
// reassignment raced that deferred commit and, under rebalance churn, reassigned the
// partition first - the new owner then reprocessed the uncommitted tail (duplicates).
//
// Exclusivity with record delivery needs no adapter-side lock: franz-go refuses
// to start this callback while the fetch loop holds a poller registration, and
// the loop holds one from fetch-return until the engine's dispatch of the
// record is complete (see the gate protocol on the Adapter fields). So when
// this drain starts, every record Poll has returned is fully dispatched and
// visible to it, and no new record can be fetched or delivered until the whole
// rebalance completes - the fetch loop is parked inside the client, while the
// engine's Poll keeps returning empty from its channel timeout, responsive to
// stop signals throughout. Any record already delivered for a revoked
// partition is handled by the engine's orphaned-work-item protection. Assigns
// carry no commit-before-release requirement and remain deferred to the poll
// loop.
func (a *Adapter) OnRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if len(revoked) == 0 {
		return
	}

	if err := a.triggerRevoke(revoked); err != nil {
		a.logger.Error(a.ctx, fmt.Sprintf("revoke drain/commit failed: %v", err))
	}
}

// OnLost should be passed to kgo.OnPartitionsLost when creating the client.
func (a *Adapter) OnLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	a.OnRevoked(ctx, nil, lost)
}

// processPendingRebalances handles queued assign events from the poll loop.
// Revokes are NOT queued here: they are drained and committed synchronously inside
// OnRevoked (franz-go blocks reassignment until that callback returns, which the
// deferred path could not guarantee). Only assigns, which have no commit-before-release
// ordering requirement, are processed here.
func (a *Adapter) processPendingRebalances() error {
	a.mu.Lock()
	assigned := a.pendingAssigned
	a.pendingAssigned = make(map[string][]int32)
	a.mu.Unlock()

	if len(assigned) > 0 {
		if err := a.triggerAssign(assigned); err != nil {
			// re-queue: the engine never accepted this assign, so it must be
			// retried on the next poll. OnAssigned may have queued more in the
			// meantime; keep the failed batch first.
			a.mu.Lock()
			for topic, partitions := range assigned {
				a.pendingAssigned[topic] = append(partitions, a.pendingAssigned[topic]...)
			}
			a.mu.Unlock()
			return err
		}
	}

	return nil
}

func (a *Adapter) triggerAssign(assigned map[string][]int32) error {
	if a.adaptedConsumer == nil {
		return nil
	}

	committed := a.lookupAssignmentOffsets(assigned)

	var info []nexus.RebalanceInfo
	for topic, partitions := range assigned {
		for _, p := range partitions {
			// The lookup normally supplies the partition's real committed offset.
			// -1 = unknown (lookup disabled, failed, or no offset committed yet).
			// Never 0: offset 0 is a valid (if rare) offset, so a zero value would
			// report a real committed position rather than the absence of one.
			// See TestRebalanceInfo_UnknownBaselineIsMinusOne.
			offset := int64(-1)
			if partitionOffsets, ok := committed[topic]; ok {
				if committedOffset, ok := partitionOffsets[p]; ok && committedOffset >= 0 {
					offset = committedOffset
				}
			}
			info = append(info, nexus.RebalanceInfo{
				RebalanceType:   nexus.Assign,
				TopicName:       topic,
				Partition:       p,
				CommittedOffset: offset,
			})
		}
	}

	a.logger.Info(a.ctx, fmt.Sprintf("partitions assigned: %v", assigned))

	// DIAGNOSTIC TRACE arming commented out for the mutex-vs-coincidence experiment (see the
	// note at the call site in broker_port.go Poll). This block takes a.mu on the assign
	// path; removing it (with the per-record call in Poll) is the point of the experiment.
	/*
		a.mu.Lock()
		if a.traceAwaitFirstRead == nil {
			a.traceAwaitFirstRead = make(map[int32]bool)
		}
		for _, partitions := range assigned {
			for _, p := range partitions {
				a.traceAwaitFirstRead[p] = true
			}
		}
		a.mu.Unlock()
	*/

	if err := a.adaptedConsumer.TriggerRebalance(nexus.Assign, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (assign): %w", err)
	}
	return nil
}

// lookupAssignmentOffsets queries the broker for the group's committed offsets
// on freshly assigned partitions: one coordinator round trip per assign event,
// batched across all topics and partitions, and only ever on assignment (zero
// steady-state cost). The result seeds RebalanceInfo.CommittedOffset with the
// real baseline instead of the -1 unknown sentinel. Failure is never fatal:
// the assign proceeds with -1 and a warning. Disable the round trip with
// WithAssignmentOffsetLookup(false).
func (a *Adapter) lookupAssignmentOffsets(assigned map[string][]int32) map[string]map[int32]int64 {
	if !a.opts.assignmentOffsetLookup || a.fetchCommittedFn == nil {
		return nil
	}

	topics := make([]string, 0, len(assigned))
	for topic := range assigned {
		topics = append(topics, topic)
	}

	// bounded: the coordinator can be mid-move during the very churn that
	// produces assigns, and the poll loop must not stall behind this
	ctx, cancel := context.WithTimeout(a.ctx, assignmentOffsetLookupTimeout)
	defer cancel()

	committed, err := a.fetchCommittedFn(ctx, topics)
	if err != nil {
		a.logger.Warn(a.ctx, fmt.Sprintf(
			"assignment offset lookup failed, proceeding with unknown (-1) baselines: %v", err))
		return nil
	}
	return committed
}

func (a *Adapter) triggerRevoke(revoked map[string][]int32) error {
	if a.adaptedConsumer == nil {
		return nil
	}

	var info []nexus.RebalanceInfo
	for topic, partitions := range revoked {
		for _, p := range partitions {
			info = append(info, nexus.RebalanceInfo{
				RebalanceType: nexus.Revoke,
				TopicName:     topic,
				Partition:     p,
				// unknown; -1 keeps the contract uniform so no reader ever meets a
				// zero value masquerading as a real offset (see triggerAssign)
				CommittedOffset: -1,
			})
		}
	}

	a.logger.Info(a.ctx, fmt.Sprintf("partitions revoked: %v", revoked))
	if err := a.adaptedConsumer.TriggerRebalance(nexus.Revoke, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (revoke): %w", err)
	}
	return nil
}
