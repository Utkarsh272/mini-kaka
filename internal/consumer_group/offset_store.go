package consumer_group

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/storage"
)

// OffsetStore persists consumer group offsets to an internal append-only log
// (mirroring Kafka's __consumer_offsets topic). On startup, the log is
// replayed forward so the latest committed offset per (group, topic,
// partition) is recovered.
//
// Each log record encodes:
//   key:   group_id_len(2) + group_id + topic_len(2) + topic + partition(4)
//   value: offset(8)
//
// A value of -1 signals a tombstone (offset deleted / group reset).
type OffsetStore struct {
	mu      sync.RWMutex
	offsets map[offsetKey]int64 // in-memory view, rebuilt on startup
	log     *storage.Log
}

type offsetKey struct {
	groupID   string
	topic     string
	partition int32
}

// NewOffsetStore opens (or creates) the offset log in dir and replays all
// existing records to rebuild the in-memory map.
func NewOffsetStore(dir string) (*OffsetStore, error) {
	log, err := storage.OpenLog(dir)
	if err != nil {
		return nil, fmt.Errorf("open offset log: %w", err)
	}

	s := &OffsetStore{
		offsets: make(map[offsetKey]int64),
		log:     log,
	}

	if err := s.replay(); err != nil {
		log.Close()
		return nil, fmt.Errorf("replay offset log: %w", err)
	}
	return s, nil
}

// Commit persists offset for (groupID, topic, partition) to the log and
// updates the in-memory map.
func (s *OffsetStore) Commit(groupID, topic string, partition int32, offset int64) error {
	key := encodeOffsetKey(groupID, topic, partition)
	value := encodeOffsetValue(offset)

	if _, err := s.log.Append(key, value); err != nil {
		return fmt.Errorf("append offset record: %w", err)
	}

	s.mu.Lock()
	s.offsets[offsetKey{groupID, topic, partition}] = offset
	s.mu.Unlock()
	return nil
}

// Fetch returns the committed offset for (groupID, topic, partition).
// Returns -1 if no offset has been committed.
func (s *OffsetStore) Fetch(groupID, topic string, partition int32) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	off, ok := s.offsets[offsetKey{groupID, topic, partition}]
	if !ok {
		return -1
	}
	return off
}

// Close flushes and closes the underlying log.
func (s *OffsetStore) Close() error {
	return s.log.Close()
}

// replay reads the entire offset log from offset 0 and rebuilds the in-memory
// map. Later records overwrite earlier ones, giving us the latest committed
// offset per key.
func (s *OffsetStore) replay() error {
	leo := s.log.LogEndOffset()
	if leo == 0 {
		return nil // empty log, nothing to replay
	}

	records, err := s.log.ReadBatch(0, 64*1024*1024) // up to 64 MB
	if err != nil {
		return err
	}

	for _, rec := range records {
		k, err := decodeOffsetKey(rec.Key)
		if err != nil {
			continue // skip malformed records
		}
		if len(rec.Value) < 8 {
			continue
		}
		offset := int64(binary.BigEndian.Uint64(rec.Value))
		if offset == -1 {
			delete(s.offsets, k) // tombstone
		} else {
			s.offsets[k] = offset
		}
	}
	return nil
}

// ---- Encoding helpers ------------------------------------------------------

// encodeOffsetKey encodes (groupID, topic, partition) into a byte slice.
// Format: [group_len: 2B][group: N][topic_len: 2B][topic: M][partition: 4B]
func encodeOffsetKey(groupID, topic string, partition int32) []byte {
	buf := make([]byte, 2+len(groupID)+2+len(topic)+4)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(groupID)))
	pos += 2
	copy(buf[pos:], groupID)
	pos += len(groupID)
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topic)))
	pos += 2
	copy(buf[pos:], topic)
	pos += len(topic)
	binary.BigEndian.PutUint32(buf[pos:], uint32(partition))
	return buf
}

// decodeOffsetKey parses an offsetKey from a byte slice.
func decodeOffsetKey(data []byte) (offsetKey, error) {
	pos := 0
	if pos+2 > len(data) {
		return offsetKey{}, fmt.Errorf("truncated key")
	}
	groupLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+groupLen > len(data) {
		return offsetKey{}, fmt.Errorf("truncated group_id")
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	if pos+2 > len(data) {
		return offsetKey{}, fmt.Errorf("truncated key (topic len)")
	}
	topicLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+topicLen > len(data) {
		return offsetKey{}, fmt.Errorf("truncated topic")
	}
	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	if pos+4 > len(data) {
		return offsetKey{}, fmt.Errorf("truncated partition")
	}
	partition := int32(binary.BigEndian.Uint32(data[pos:]))

	return offsetKey{groupID, topic, partition}, nil
}

// encodeOffsetValue encodes an int64 offset as 8 big-endian bytes.
func encodeOffsetValue(offset int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	return buf
}
