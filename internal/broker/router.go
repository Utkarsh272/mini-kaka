package broker

import (
	"encoding/binary"
	"hash/fnv"
	"sync/atomic"
)

// roundRobinCounter is a per-topic atomic counter for keyless produce routing.
type roundRobinCounter struct {
	val atomic.Uint64
}

func (c *roundRobinCounter) next(numPartitions int32) int32 {
	n := c.val.Add(1) - 1
	return int32(n % uint64(numPartitions))
}

// RouteKey selects the target partition for a record.
//
//   - Non-nil, non-empty key: FNV-1a hash of key % numPartitions.
//     Same key always maps to the same partition (sticky routing), preserving
//     per-key ordering guarantees.
//   - Nil or empty key: round-robin via counter, spreading load evenly.
//     counter may be nil when calling with a keyed record (it is not used).
//
// This mirrors Kafka's DefaultPartitioner behaviour.
func RouteKey(key []byte, numPartitions int32, counter *roundRobinCounter) int32 {
	if numPartitions <= 1 {
		return 0
	}
	if len(key) == 0 {
		if counter == nil {
			return 0
		}
		return counter.next(numPartitions)
	}
	return hashKey(key, numPartitions)
}

// hashKey computes FNV-1a (32-bit) of key and returns key % numPartitions.
// Result is always in [0, numPartitions).
func hashKey(key []byte, numPartitions int32) int32 {
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(numPartitions))
}

// partitionForNumericKey routes 8-byte big-endian numeric keys by value mod N.
// Useful when key IS a numeric ID — avoids the hash overhead.
// Not wired to the default path but exported for benchmarks.
func partitionForNumericKey(key []byte, numPartitions int32) int32 {
	if len(key) != 8 {
		return hashKey(key, numPartitions)
	}
	n := binary.BigEndian.Uint64(key)
	return int32(n % uint64(numPartitions))
}

var _ = partitionForNumericKey // keep linter happy

// ---- Test helpers (exported only for *_test.go in external test packages) --

// ExportedRouteKey exposes RouteKey for use in broker_test package tests.
func ExportedRouteKey(key []byte, numPartitions int32, counter *roundRobinCounter) int32 {
	return RouteKey(key, numPartitions, counter)
}

// NewRoundRobinCounter creates a fresh round-robin counter for tests.
func NewRoundRobinCounter() *roundRobinCounter {
	return &roundRobinCounter{}
}
