// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CompatibilityBalancers returns a kgo.Opt that configures fallback balancers for
// maximum compatibility with existing consumer groups.
//
// Use this when joining groups that may have consumers using different rebalance
// protocols (e.g., legacy Java consumers using range/roundrobin, or mixed deployments
// during migration to cooperative rebalancing).
//
// Balancer priority (first compatible with all group members wins):
//
//  1. CooperativeStickyBalancer - incremental rebalancing, minimal disruption
//  2. StickyBalancer - eager protocol, but minimises partition movement
//  3. RoundRobinBalancer - eager protocol, simple and widely supported
//  4. RangeBalancer - eager protocol, Kafka's default, maximum compatibility
//
// When a fallback balancer is selected (because higher-priority balancers are not
// supported by all group members), a warning is logged explaining which preferred
// balancers were unavailable.
//
// Example usage:
//
//	adapter := franzadapter.NewWithOptions(ctx, "my-group",
//	    []string{"broker:9092"},
//	    []kgo.Opt{
//	        franzadapter.CompatibilityBalancers(),
//	    },
//	)
//
// For new deployments where all consumers use this adapter, the default
// CooperativeStickyBalancer (used by New/NewWithOptions without this option)
// is recommended for optimal rebalance performance.
func CompatibilityBalancers() kgo.Opt {
	balancers := []kgo.GroupBalancer{
		kgo.CooperativeStickyBalancer(),
		kgo.StickyBalancer(),
		kgo.RoundRobinBalancer(),
		kgo.RangeBalancer(),
	}

	wrapped := make([]kgo.GroupBalancer, len(balancers))
	for i, b := range balancers {
		skipped := make([]string, i)
		for j := 0; j < i; j++ {
			skipped[j] = balancers[j].ProtocolName()
		}
		wrapped[i] = &loggingBalancer{
			GroupBalancer: b,
			index:         i,
			skipped:       skipped,
		}
	}

	return kgo.Balancers(wrapped...)
}

// loggingBalancer wraps a kgo.GroupBalancer to log when it's selected during
// consumer group protocol negotiation.
//
// The Kafka consumer group protocol requires all members to agree on a common
// assignor. When a consumer joins a group, it advertises which protocols it
// supports. The group coordinator selects a protocol supported by ALL members.
//
// This wrapper detects when its balancer is selected (via ParseSyncAssignment)
// and logs appropriately:
//   - Index 0 (preferred): INFO log confirming optimal protocol in use
//   - Index > 0 (fallback): WARN log explaining which preferred protocols
//     were not supported by all group members
type loggingBalancer struct {
	kgo.GroupBalancer
	index   int      // 0 = preferred, >0 = fallback position
	skipped []string // protocol names of higher-priority balancers not available
	logged  sync.Once
}

// ParseSyncAssignment is called when this consumer receives its partition assignment.
// This method is only called on the balancer that was selected during negotiation,
// making it the ideal hook for logging which protocol is in use.
func (lb *loggingBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	lb.logged.Do(func() {
		name := lb.ProtocolName()
		cooperative := lb.IsCooperative()

		if lb.index == 0 {
			// preferred balancer selected - log at info level
			if cooperative {
				log.Printf("franzadapter: using %s balancer (cooperative); "+
					"only affected partitions revoked during rebalance", name)
			} else {
				log.Printf("franzadapter: using %s balancer (eager); "+
					"all partitions revoked during rebalance", name)
			}
		} else {
			// fallback balancer selected - warn and explain why
			protocolType := "eager"
			if cooperative {
				protocolType = "cooperative"
			}
			log.Printf("franzadapter: WARNING: using %s balancer (%s) as fallback; "+
				"%s not supported by all group members",
				name, protocolType, strings.Join(lb.skipped, ", "))
		}
	})

	result, err := lb.GroupBalancer.ParseSyncAssignment(assignment)
	if err != nil {
		return nil, fmt.Errorf("parse sync assignment: %w", err)
	}
	return result, nil
}
