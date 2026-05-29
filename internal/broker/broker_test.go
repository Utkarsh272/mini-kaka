package broker_test

import (
	"fmt"
	"testing"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
)

// ---- Router tests ----------------------------------------------------------

func TestRouteKeySticky(t *testing.T) {
	// Same key must always route to the same partition across many calls.
	// We use a fresh counter (zero value) for each call since keyed routing
	// doesn't use the counter.
	key := []byte("user-12345")
	numPartitions := int32(8)

	first := broker.ExportedRouteKey(key, numPartitions, nil)
	for i := 0; i < 1000; i++ {
		got := broker.ExportedRouteKey(key, numPartitions, nil)
		if got != first {
			t.Errorf("iteration %d: key routed to %d, want %d", i, got, first)
		}
	}
}

func TestRouteKeyInRange(t *testing.T) {
	numPartitions := int32(6)
	keys := [][]byte{
		[]byte("alpha"), []byte("beta"), []byte("gamma"),
		[]byte("delta"), []byte("epsilon"), []byte("zeta"),
		[]byte("a"), []byte("z"), []byte("zzzzzzzzzzzzzzz"),
	}
	for _, key := range keys {
		p := broker.ExportedRouteKey(key, numPartitions, nil)
		if p < 0 || p >= numPartitions {
			t.Errorf("key %q routed to %d, outside [0,%d)", key, p, numPartitions)
		}
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	// Keyless records should spread across all partitions.
	numPartitions := int32(4)
	counts := make(map[int32]int)
	counter := broker.NewRoundRobinCounter()

	const total = 100
	for i := 0; i < total; i++ {
		p := broker.ExportedRouteKey(nil, numPartitions, counter)
		counts[p]++
	}

	// Each partition should have received exactly total/numPartitions records.
	expected := total / int(numPartitions)
	for p := int32(0); p < numPartitions; p++ {
		if counts[p] != expected {
			t.Errorf("partition %d got %d records, want %d", p, counts[p], expected)
		}
	}
}

func TestRoundRobinSinglePartition(t *testing.T) {
	counter := broker.NewRoundRobinCounter()
	for i := 0; i < 10; i++ {
		p := broker.ExportedRouteKey(nil, 1, counter)
		if p != 0 {
			t.Errorf("single partition: got %d, want 0", p)
		}
	}
}

func TestDifferentKeysDifferentPartitions(t *testing.T) {
	// With enough partitions, different keys should map to different partitions
	// (not guaranteed for all pairs, but should hold for well-separated keys).
	numPartitions := int32(16)
	seen := make(map[int32]string)
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("distinct-key-%d", i))
		p := broker.ExportedRouteKey(key, numPartitions, nil)
		seen[p] = string(key)
	}
	// We should see at least half the partitions used with 50 distinct keys.
	if len(seen) < int(numPartitions/2) {
		t.Errorf("only %d/%d partitions used — hash distribution is poor", len(seen), numPartitions)
	}
}

// ---- Broker + metadata persistence tests -----------------------------------

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	b, err := broker.NewBroker(1, "localhost", 9092, dir)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestCreateAndGetPartition(t *testing.T) {
	b := newTestBroker(t)

	if err := b.CreateTopic("orders", 3, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for partID := int32(0); partID < 3; partID++ {
		p, err := b.GetPartition("orders", partID)
		if err != nil {
			t.Errorf("GetPartition(%d): %v", partID, err)
			continue
		}
		off, err := p.Append([]byte("k"), []byte("v"))
		if err != nil {
			t.Errorf("Append to partition %d: %v", partID, err)
		}
		if off != 0 {
			t.Errorf("first offset in partition %d = %d, want 0", partID, off)
		}
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	b := newTestBroker(t)
	if err := b.CreateTopic("dup", 1, 1); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := b.CreateTopic("dup", 1, 1); err == nil {
		t.Error("expected error on duplicate CreateTopic, got nil")
	}
}

func TestGetPartitionUnknownTopic(t *testing.T) {
	b := newTestBroker(t)
	if _, err := b.GetPartition("ghost", 0); err == nil {
		t.Error("expected error for unknown topic, got nil")
	}
}

func TestGetPartitionOutOfRange(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("small", 2, 1)
	if _, err := b.GetPartition("small", 5); err == nil {
		t.Error("expected error for out-of-range partition, got nil")
	}
}

func TestMetadataPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create broker, add topics, close.
	{
		b, err := broker.NewBroker(1, "localhost", 9092, dir)
		if err != nil {
			t.Fatalf("NewBroker: %v", err)
		}
		b.CreateTopic("events", 3, 1)
		b.CreateTopic("logs", 1, 1)

		// Write some records so the log files have content.
		p, _ := b.GetPartition("events", 0)
		p.Append([]byte("k"), []byte("hello"))

		b.Close()
	}

	// Reopen broker — topics must be recovered from bbolt.
	{
		b, err := broker.NewBroker(1, "localhost", 9092, dir)
		if err != nil {
			t.Fatalf("reopen NewBroker: %v", err)
		}
		defer b.Close()

		topics := b.ListTopics()
		nameSet := make(map[string]bool)
		for _, t := range topics {
			nameSet[t.Name()] = true
		}
		if !nameSet["events"] {
			t.Error("topic 'events' not recovered after restart")
		}
		if !nameSet["logs"] {
			t.Error("topic 'logs' not recovered after restart")
		}

		// Verify the partition log was also recovered (LEO should be 1).
		p, err := b.GetPartition("events", 0)
		if err != nil {
			t.Fatalf("GetPartition after restart: %v", err)
		}
		if leo := p.LogEndOffset(); leo != 1 {
			t.Errorf("recovered LEO = %d, want 1", leo)
		}

		// Append a new record — should get offset 1.
		off, err := p.Append([]byte("k"), []byte("world"))
		if err != nil {
			t.Fatalf("Append after restart: %v", err)
		}
		if off != 1 {
			t.Errorf("post-recovery offset = %d, want 1", off)
		}
	}
}

func TestRoutePartition(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("routed", 4, 1)

	// Same key always routes to same partition.
	key := []byte("stable-key")
	first, err := b.RoutePartition("routed", key)
	if err != nil {
		t.Fatalf("RoutePartition: %v", err)
	}
	for i := 0; i < 50; i++ {
		got, _ := b.RoutePartition("routed", key)
		if got != first {
			t.Errorf("key routing not stable: got %d, want %d", got, first)
		}
	}

	// Nil key uses round-robin — should cover all 4 partitions in 8 calls.
	seen := make(map[int32]bool)
	for i := 0; i < 100; i++ {
		p, _ := b.RoutePartition("routed", nil)
		seen[p] = true
	}
	if len(seen) != 4 {
		t.Errorf("round-robin covered %d/4 partitions", len(seen))
	}
}

func TestRoutePartitionUnknownTopic(t *testing.T) {
	b := newTestBroker(t)
	if _, err := b.RoutePartition("unknown", []byte("k")); err == nil {
		t.Error("expected error routing to unknown topic, got nil")
	}
}

func TestListTopics(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("a", 1, 1)
	b.CreateTopic("b", 2, 1)
	b.CreateTopic("c", 3, 1)

	topics := b.ListTopics()
	if len(topics) != 3 {
		t.Errorf("ListTopics = %d, want 3", len(topics))
	}
}

func TestTopicExists(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("exists", 1, 1)

	if !b.TopicExists("exists") {
		t.Error("TopicExists('exists') = false, want true")
	}
	if b.TopicExists("ghost") {
		t.Error("TopicExists('ghost') = true, want false")
	}
}

func TestCommitAndFetchOffset(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("commits", 1, 1)

	p, _ := b.GetPartition("commits", 0)

	// No commit yet → -1.
	if off := p.FetchOffset("group-A"); off != -1 {
		t.Errorf("initial committed offset = %d, want -1", off)
	}

	p.CommitOffset("group-A", 42)
	if off := p.FetchOffset("group-A"); off != 42 {
		t.Errorf("committed offset = %d, want 42", off)
	}

	// Different group should still be -1.
	if off := p.FetchOffset("group-B"); off != -1 {
		t.Errorf("group-B offset = %d, want -1", off)
	}
}
