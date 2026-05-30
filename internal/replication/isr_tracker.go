package replication

import (
	"sync"
	"time"
)

const (
	defaultLagTimeMax    = 30 * time.Second
	defaultLagRecordsMax = int64(10_000)
)

// ReplicaState tracks the replication state of one follower replica.
type ReplicaState struct {
	NodeID        int32
	FetchOffset   int64
	LastFetchTime time.Time
	InISR         bool
}

// ISRTracker tracks the in-sync replica set for one partition on the leader.
type ISRTracker struct {
	mu sync.RWMutex

	leaderNodeID  int32
	replicas      map[int32]*ReplicaState
	lagTimeMax    time.Duration
	lagRecordsMax int64
	getLEO        func() int64
}

// NewISRTracker creates an ISRTracker with default lag thresholds.
func NewISRTracker(
	leaderNodeID int32,
	initialReplicas []int32,
	getLEO func() int64,
) *ISRTracker {
	return NewISRTrackerWithConfig(leaderNodeID, initialReplicas, getLEO,
		defaultLagTimeMax, defaultLagRecordsMax)
}

// NewISRTrackerWithConfig creates an ISRTracker with custom lag thresholds.
// Exposed for tests that need short timeouts.
func NewISRTrackerWithConfig(
	leaderNodeID int32,
	initialReplicas []int32,
	getLEO func() int64,
	lagTimeMax time.Duration,
	lagRecordsMax int64,
) *ISRTracker {
	t := &ISRTracker{
		leaderNodeID:  leaderNodeID,
		replicas:      make(map[int32]*ReplicaState),
		lagTimeMax:    lagTimeMax,
		lagRecordsMax: lagRecordsMax,
		getLEO:        getLEO,
	}
	for _, nodeID := range initialReplicas {
		t.replicas[nodeID] = &ReplicaState{
			NodeID:        nodeID,
			FetchOffset:   0,
			LastFetchTime: time.Now(),
			InISR:         true,
		}
	}
	return t
}

// RecordFetch updates a follower's fetch progress by node ID and re-evaluates
// ISR membership. Use this when the follower's node ID is known.
func (t *ISRTracker) RecordFetch(followerNodeID int32, fetchedUpTo int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	r, ok := t.replicas[followerNodeID]
	if !ok {
		r = &ReplicaState{NodeID: followerNodeID}
		t.replicas[followerNodeID] = r
	}

	r.FetchOffset = fetchedUpTo
	r.LastFetchTime = time.Now()
	r.InISR = t.isEligible(r, t.getLEO())
}

// HighWatermark returns the minimum fetch offset across all ISR members.
// Consumers may only read records up to the high-watermark.
func (t *ISRTracker) HighWatermark() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leo := t.getLEO()
	hwm := leo

	for _, r := range t.replicas {
		if !r.InISR {
			continue
		}
		if r.NodeID == t.leaderNodeID {
			continue
		}
		if r.FetchOffset < hwm {
			hwm = r.FetchOffset
		}
	}

	if hwm < 0 {
		hwm = 0
	}
	return hwm
}

// ShrinkISR removes followers that exceed lag thresholds from the ISR.
func (t *ISRTracker) ShrinkISR() {
	t.mu.Lock()
	defer t.mu.Unlock()

	leo := t.getLEO()
	for _, r := range t.replicas {
		if r.NodeID == t.leaderNodeID {
			continue
		}
		r.InISR = t.isEligible(r, leo)
	}
}

// ISRMembers returns the current list of in-sync replica node IDs.
func (t *ISRTracker) ISRMembers() []int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var members []int32
	for _, r := range t.replicas {
		if r.InISR {
			members = append(members, r.NodeID)
		}
	}
	return members
}

// ReplicaStates returns a snapshot of all replica states for observability.
func (t *ISRTracker) ReplicaStates() []ReplicaState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	states := make([]ReplicaState, 0, len(t.replicas))
	for _, r := range t.replicas {
		states = append(states, *r)
	}
	return states
}

func (t *ISRTracker) isEligible(r *ReplicaState, leaderLEO int64) bool {
	if time.Since(r.LastFetchTime) > t.lagTimeMax {
		return false
	}
	if leaderLEO-r.FetchOffset > t.lagRecordsMax {
		return false
	}
	return true
}
