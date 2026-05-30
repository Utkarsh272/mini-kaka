package broker

import (
	"fmt"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/consumer_group"
	"github.com/Utkarsh272/mini-kafka/internal/replication"
	"github.com/Utkarsh272/mini-kafka/internal/storage"
)

// Partition holds the append-only log for one partition, its ISR tracker
// (leader only), its follower fetcher (follower only), and in-memory consumer
// group offset tracking.
type Partition struct {
	mu               sync.RWMutex
	id               int32
	log              *storage.Log
	committedOffsets map[string]int64

	// ISR tracker — non-nil only on the leader replica.
	isr *replication.ISRTracker

	// Follower fetcher — non-nil only on follower replicas.
	fetcher *replication.FollowerFetcher
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

// HighWatermark returns the high-watermark:
//   - If this partition has an ISR tracker (leader), it is the minimum LEO
//     across all in-sync replicas.
//   - Otherwise (follower or single-broker), it equals the LEO.
func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	isr := p.isr
	p.mu.RUnlock()

	if isr != nil {
		return isr.HighWatermark()
	}
	return p.LogEndOffset()
}

// ISRTracker returns the partition's ISR tracker, or nil if this is a follower.
func (p *Partition) ISRTracker() *replication.ISRTracker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isr
}

// SetISRTracker attaches an ISR tracker to this partition (called on the leader).
func (p *Partition) SetISRTracker(t *replication.ISRTracker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isr = t
}

// SetFollowerFetcher attaches a follower fetcher and starts it.
func (p *Partition) SetFollowerFetcher(f *replication.FollowerFetcher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fetcher = f
	f.Start()
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

// stopReplication stops the follower fetcher if running.
func (p *Partition) stopReplication() {
	p.mu.Lock()
	f := p.fetcher
	p.mu.Unlock()
	if f != nil {
		f.Stop()
	}
}

// Topic holds all partitions for one topic.
type Topic struct {
	name              string
	partitions        []*Partition
	replicationFactor int32
	rrCounter         roundRobinCounter
}

func (t *Topic) Name() string             { return t.name }
func (t *Topic) NumPartitions() int       { return len(t.partitions) }
func (t *Topic) ReplicationFactor() int32 { return t.replicationFactor }
func (t *Topic) Partition(id int32) *Partition {
	if id < 0 || int(id) >= len(t.partitions) {
		return nil
	}
	return t.partitions[id]
}
func (t *Topic) RoutePartition(key []byte) int32 {
	return RouteKey(key, int32(len(t.partitions)), &t.rrCounter)
}

// Broker is the central coordinator.
type Broker struct {
	mu          sync.RWMutex
	nodeID      int32
	host        string
	port        int32
	dataDir     string
	topics      map[string]*Topic
	metadata    *metadataStore
	Coordinator *consumer_group.Coordinator
	OffsetStore *consumer_group.OffsetStore
}

// NewBroker creates a Broker, opens bbolt metadata, replays topics, and
// initialises the consumer group coordinator + offset store.
func NewBroker(nodeID int32, host string, port int32, dataDir string) (*Broker, error) {
	meta, err := openMetadataStore(dataDir + "/meta.db")
	if err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}

	offsetStore, err := consumer_group.NewOffsetStore(dataDir + "/__consumer_offsets")
	if err != nil {
		meta.close()
		return nil, fmt.Errorf("open offset store: %w", err)
	}

	b := &Broker{
		nodeID:      nodeID,
		host:        host,
		port:        port,
		dataDir:     dataDir,
		topics:      make(map[string]*Topic),
		metadata:    meta,
		OffsetStore: offsetStore,
	}

	b.Coordinator = consumer_group.NewCoordinator(func(topic string) (int32, error) {
		b.mu.RLock()
		defer b.mu.RUnlock()
		t, ok := b.topics[topic]
		if !ok {
			return 0, fmt.Errorf("unknown topic %q", topic)
		}
		return int32(t.NumPartitions()), nil
	})

	if err := b.replayMetadata(); err != nil {
		meta.close()
		offsetStore.Close()
		return nil, fmt.Errorf("replay metadata: %w", err)
	}

	return b, nil
}

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

func (b *Broker) openTopic(name string, numPartitions, replicationFactor int32) (*Topic, error) {
	t := &Topic{name: name, replicationFactor: replicationFactor}
	for i := int32(0); i < numPartitions; i++ {
		dir := fmt.Sprintf("%s/%s-%d", b.dataDir, name, i)
		log, err := storage.OpenLog(dir)
		if err != nil {
			for _, p := range t.partitions {
				p.log.Close()
			}
			return nil, fmt.Errorf("open partition log %s-%d: %w", name, i, err)
		}
		t.partitions = append(t.partitions, newPartition(i, log))
	}
	return t, nil
}

// InitLeaderISR attaches an ISR tracker to a partition, designating this
// broker as the leader for that partition. replicaNodeIDs should include this
// broker's nodeID plus all follower node IDs.
func (b *Broker) InitLeaderISR(topic string, partitionID int32, replicaNodeIDs []int32) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.topics[topic]
	if !ok {
		return fmt.Errorf("unknown topic %q", topic)
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return fmt.Errorf("unknown partition %d", partitionID)
	}

	p := t.partitions[partitionID]
	tracker := replication.NewISRTracker(
		b.nodeID,
		replicaNodeIDs,
		func() int64 { return p.LogEndOffset() },
	)
	p.SetISRTracker(tracker)
	return nil
}

// InitFollowerFetcher starts a follower fetcher for a partition, designating
// this broker as a follower that replicates from leaderAddr.
func (b *Broker) InitFollowerFetcher(topic string, partitionID int32, leaderAddr string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.topics[topic]
	if !ok {
		return fmt.Errorf("unknown topic %q", topic)
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return fmt.Errorf("unknown partition %d", partitionID)
	}

	p := t.partitions[partitionID]
	fetcher := replication.NewFollowerFetcher(
		b.nodeID,
		leaderAddr,
		topic,
		partitionID,
		p.log,
		nil, // onFetch not needed — leader learns offset from the request itself
	)
	p.SetFollowerFetcher(fetcher)
	return nil
}

func (b *Broker) NodeID() int32 { return b.nodeID }
func (b *Broker) Host() string  { return b.host }
func (b *Broker) Port() int32   { return b.port }

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

	if err := b.metadata.saveTopic(topicConfig{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}); err != nil {
		for _, p := range t.partitions {
			p.log.Close()
		}
		return fmt.Errorf("persist topic %q: %w", name, err)
	}

	b.topics[name] = t
	return nil
}

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

func (b *Broker) RoutePartition(topic string, key []byte) (int32, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.topics[topic]
	if !ok {
		return 0, fmt.Errorf("unknown topic %q", topic)
	}
	return t.RoutePartition(key), nil
}

func (b *Broker) ListTopics() []*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	return topics
}

func (b *Broker) TopicExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.topics[name]
	return ok
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.topics {
		for _, p := range t.partitions {
			p.stopReplication()
			if err := p.log.Close(); err != nil {
				return err
			}
		}
	}
	if err := b.OffsetStore.Close(); err != nil {
		return err
	}
	return b.metadata.close()
}
