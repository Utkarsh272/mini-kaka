package broker

import (
	"fmt"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/storage"
)

// Partition holds the append-only log for one partition plus in-memory
// consumer group offset tracking.
type Partition struct {
	mu               sync.RWMutex
	id               int32
	log              *storage.Log
	committedOffsets map[string]int64 // groupID → last committed offset
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

// ReadBatch returns up to maxBytes of records starting at startOffset.
func (p *Partition) ReadBatch(startOffset int64, maxBytes int64) ([]storage.Record, error) {
	if startOffset < 0 {
		startOffset = 0
	}
	return p.log.ReadBatch(uint64(startOffset), maxBytes)
}

// LogEndOffset returns the next offset to be assigned (LEO).
func (p *Partition) LogEndOffset() int64 {
	return int64(p.log.LogEndOffset())
}

// HighWatermark is the LEO for single-broker mode. Replication (Days 10-12)
// will hold this at min(ISR logEndOffsets).
func (p *Partition) HighWatermark() int64 {
	return p.LogEndOffset()
}

// CommitOffset stores the consumed offset for a consumer group.
func (p *Partition) CommitOffset(groupID string, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.committedOffsets[groupID] = offset
}

// FetchOffset returns the committed offset for a group, or -1 if none exists.
func (p *Partition) FetchOffset(groupID string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	off, ok := p.committedOffsets[groupID]
	if !ok {
		return -1
	}
	return off
}

// Topic holds all partitions for one topic plus its round-robin router state.
type Topic struct {
	name              string
	partitions        []*Partition
	replicationFactor int32
	rrCounter         roundRobinCounter // for keyless produce routing
}

// Name returns the topic name.
func (t *Topic) Name() string { return t.name }

// NumPartitions returns the number of partitions.
func (t *Topic) NumPartitions() int { return len(t.partitions) }

// ReplicationFactor returns the configured replication factor.
func (t *Topic) ReplicationFactor() int32 { return t.replicationFactor }

// Partition returns the partition by id, or nil if out of range.
func (t *Topic) Partition(id int32) *Partition {
	if id < 0 || int(id) >= len(t.partitions) {
		return nil
	}
	return t.partitions[id]
}

// RoutePartition selects the target partition for a record:
//   - non-empty key → FNV-1a hash % numPartitions (sticky, order-preserving)
//   - nil/empty key → round-robin (even load distribution)
func (t *Topic) RoutePartition(key []byte) int32 {
	return RouteKey(key, int32(len(t.partitions)), &t.rrCounter)
}

// Broker is the central coordinator. It owns all topics and their partition
// logs and persists topic metadata to bbolt so topics survive restarts.
type Broker struct {
	mu       sync.RWMutex
	nodeID   int32
	host     string
	port     int32
	dataDir  string
	topics   map[string]*Topic
	metadata *metadataStore
}

// NewBroker creates a Broker and opens the bbolt metadata store.
// On startup it replays all persisted topics so partition logs are reopened
// before the server accepts connections.
func NewBroker(nodeID int32, host string, port int32, dataDir string) (*Broker, error) {
	meta, err := openMetadataStore(dataDir + "/meta.db")
	if err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}

	b := &Broker{
		nodeID:   nodeID,
		host:     host,
		port:     port,
		dataDir:  dataDir,
		topics:   make(map[string]*Topic),
		metadata: meta,
	}

	// Replay persisted topics — reopens all partition logs from disk.
	if err := b.replayMetadata(); err != nil {
		meta.close()
		return nil, fmt.Errorf("replay metadata: %w", err)
	}

	return b, nil
}

// replayMetadata loads all topic configs from bbolt and opens their partition
// logs. Called once at startup with no concurrent access, so no lock needed.
func (b *Broker) replayMetadata() error {
	configs, err := b.metadata.loadTopics()
	if err != nil {
		return err
	}
	for _, cfg := range configs {
		t, err := b.openTopic(cfg.Name, cfg.NumPartitions, cfg.ReplicationFactor)
		if err != nil {
			return fmt.Errorf("replay topic %q: %w", cfg.Name, err)
		}
		b.topics[cfg.Name] = t
	}
	return nil
}

// openTopic opens (or creates) partition log directories for a topic and
// returns a fully initialised *Topic. Does NOT persist to bbolt — callers
// that need persistence call metadata.saveTopic separately.
func (b *Broker) openTopic(name string, numPartitions, replicationFactor int32) (*Topic, error) {
	t := &Topic{
		name:              name,
		replicationFactor: replicationFactor,
	}
	for i := int32(0); i < numPartitions; i++ {
		dir := fmt.Sprintf("%s/%s-%d", b.dataDir, name, i)
		log, err := storage.OpenLog(dir)
		if err != nil {
			// Best-effort close already-opened partitions.
			for _, p := range t.partitions {
				p.log.Close()
			}
			return nil, fmt.Errorf("open partition log %s-%d: %w", name, i, err)
		}
		t.partitions = append(t.partitions, newPartition(i, log))
	}
	return t, nil
}

// NodeID returns the broker's node ID.
func (b *Broker) NodeID() int32 { return b.nodeID }

// Host returns the broker's advertised hostname.
func (b *Broker) Host() string { return b.host }

// Port returns the broker's TCP port.
func (b *Broker) Port() int32 { return b.port }

// CreateTopic creates a topic with numPartitions partitions and persists it to
// bbolt. Returns an error if the topic already exists.
func (b *Broker) CreateTopic(name string, numPartitions, replicationFactor int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	t, err := b.openTopic(name, numPartitions, replicationFactor)
	if err != nil {
		return err
	}

	// Persist before exposing — if the broker crashes between these two lines,
	// the log directories exist but bbolt has no record; on next startup the
	// orphaned dirs are ignored (harmless). The reverse (bbolt has record but
	// no dirs) would cause an error on replay, which is why we open first.
	if err := b.metadata.saveTopic(topicConfig{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}); err != nil {
		// Roll back the in-memory state.
		for _, p := range t.partitions {
			p.log.Close()
		}
		return fmt.Errorf("persist topic %q: %w", name, err)
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

// RoutePartition selects the target partition for a record destined for topic.
// Returns ErrUnknownTopicPartition-style error if the topic doesn't exist.
func (b *Broker) RoutePartition(topic string, key []byte) (int32, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.topics[topic]
	if !ok {
		return 0, fmt.Errorf("unknown topic %q", topic)
	}
	return t.RoutePartition(key), nil
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

// Close shuts down all partition logs and the metadata store cleanly.
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
	return b.metadata.close()
}
