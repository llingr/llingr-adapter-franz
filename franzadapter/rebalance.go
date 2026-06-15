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
func (a *Adapter) OnRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	a.mu.Lock()
	for topic, partitions := range revoked {
		a.pendingRevoked[topic] = append(a.pendingRevoked[topic], partitions...)
	}
	a.mu.Unlock()
}

// OnLost should be passed to kgo.OnPartitionsLost when creating the client.
func (a *Adapter) OnLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	a.OnRevoked(ctx, nil, lost)
}

// processPendingRebalances handles queued rebalance events.
func (a *Adapter) processPendingRebalances() error {
	a.mu.Lock()
	assigned := a.pendingAssigned
	revoked := a.pendingRevoked
	a.pendingAssigned = make(map[string][]int32)
	a.pendingRevoked = make(map[string][]int32)
	a.mu.Unlock()

	// process revokes first (important for cooperative rebalancing)
	if len(revoked) > 0 {
		if err := a.triggerRevoke(revoked); err != nil {
			return err
		}
	}

	if len(assigned) > 0 {
		if err := a.triggerAssign(assigned); err != nil {
			return err
		}
	}

	return nil
}

func (a *Adapter) triggerAssign(assigned map[string][]int32) error {
	if a.adaptedConsumer == nil {
		return nil
	}

	var info []nexus.RebalanceInfo
	for topic, partitions := range assigned {
		for _, p := range partitions {
			info = append(info, nexus.RebalanceInfo{
				RebalanceType: nexus.Assign,
				TopicName:     topic,
				Partition:     p,
			})
		}
	}

	a.logger.Info(a.ctx, fmt.Sprintf("partitions assigned: %v", assigned))
	if err := a.adaptedConsumer.TriggerRebalance(nexus.Assign, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (assign): %w", err)
	}
	return nil
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
			})
		}
	}

	a.logger.Info(a.ctx, fmt.Sprintf("partitions revoked: %v", revoked))
	if err := a.adaptedConsumer.TriggerRebalance(nexus.Revoke, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (revoke): %w", err)
	}
	return nil
}
