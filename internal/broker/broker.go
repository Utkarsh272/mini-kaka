package broker

import (
	"fmt"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/storage"
)

// Partition holds the log for one partition and its committed consumer offsets.
type Partition struct {
	mu               sync.RWMutex
	id               int32
	log              *storage.Log
	committedOffsets map[string]int64 // groupID → committed offset
}

func newPartition(id int32, log *storage.Log) *Partition {
	return &Partition{
		id:               id,
		log:              log,
		committedOffsets: make(map[string]int64),
	}
}

// Append writes key+value to the partition log and returns the assigned offset.
func (p *Partition) Append(key, value []byte) (int64, error) {
	off, err := p.log.Append(key, value)
	return int64(off), err
}

// ReadBatch returns records starting at startOffset, up to maxBytes.
func (p *Partition) ReadBatch(startOffset int64, maxBytes int64) ([]storage.Record, error) {
	if startOffset < 0 {
		startOffset = 0
	}
	return p.log.ReadBatch(uint64(startOffset), maxBytes)
}

// LogEndOffset returns the next offset to be written (LEO).
func (p *Partition) LogEndOffset() int64 {
	return int64(p.log.LogEndOffset())
}

// HighWatermark returns the high-watermark offset. For a single-broker setup
// this equals the LEO. Replication (Days 10-12) will adjust this when
// followers fall behind.
func (p *Partition) HighWatermark() int64 {
	return p.LogEndOffset()
}

// CommitOffset stores the consumed offset for a consumer group.
func (p *Partition) CommitOffset(groupID string, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.committedOffsets[groupID] = offset
}

// FetchOffset returns the committed offset for a group, or -1 if none.
func (p *Partition) FetchOffset(groupID string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	off, ok := p.committedOffsets[groupID]
	if !ok {
		return -1
	}
	return off
}

// Topic holds all partitions for one topic.
type Topic struct {
	name       string
	partitions []*Partition
}

// Name returns the topic name.
func (t *Topic) Name() string { return t.name }

// NumPartitions returns the number of partitions in the topic.
func (t *Topic) NumPartitions() int { return len(t.partitions) }

// Partition returns partition by id, or nil if out of range.
func (t *Topic) Partition(id int32) *Partition {
	if id < 0 || int(id) >= len(t.partitions) {
		return nil
	}
	return t.partitions[id]
}

// Broker is the central coordinator. It owns all topics and their partition
// logs. For Days 3-4 it is single-broker (no replication or leader election).
type Broker struct {
	mu      sync.RWMutex
	nodeID  int32
	host    string
	port    int32
	dataDir string
	topics  map[string]*Topic
}

// NewBroker creates a Broker. dataDir is where partition log files live.
func NewBroker(nodeID int32, host string, port int32, dataDir string) *Broker {
	return &Broker{
		nodeID:  nodeID,
		host:    host,
		port:    port,
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
	}
}

// NodeID returns the broker's node ID.
func (b *Broker) NodeID() int32 { return b.nodeID }

// Host returns the broker's advertised hostname.
func (b *Broker) Host() string { return b.host }

// Port returns the broker's TCP port.
func (b *Broker) Port() int32 { return b.port }

// CreateTopic creates a topic with numPartitions partitions. Returns an error
// if the topic already exists.
func (b *Broker) CreateTopic(name string, numPartitions int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	t := &Topic{name: name}
	for i := int32(0); i < numPartitions; i++ {
		dir := fmt.Sprintf("%s/%s-%d", b.dataDir, name, i)
		log, err := storage.OpenLog(dir)
		if err != nil {
			for _, p := range t.partitions {
				p.log.Close()
			}
			return fmt.Errorf("open partition log %s-%d: %w", name, i, err)
		}
		t.partitions = append(t.partitions, newPartition(i, log))
	}

	b.topics[name] = t
	return nil
}

// GetPartition returns the Partition for (topic, partitionID).
func (b *Broker) GetPartition(topic string, partitionID int32) (*Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.topics[topic]
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topic)
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return nil, fmt.Errorf("unknown partition %d for topic %q", partitionID, topic)
	}
	return t.partitions[partitionID], nil
}

// ListTopics returns a snapshot of all known topics.
func (b *Broker) ListTopics() []*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	return topics
}

// TopicExists reports whether the named topic is known.
func (b *Broker) TopicExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.topics[name]
	return ok
}

// Close shuts down all partition logs cleanly.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, p := range t.partitions {
			if err := p.log.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
