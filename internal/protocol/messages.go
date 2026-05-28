package protocol

import (
	"encoding/binary"
	"fmt"
)

// ---- Produce ---------------------------------------------------------------

// ProduceRecord is a single record within a Produce request.
type ProduceRecord struct {
	Key   []byte // nil = no key
	Value []byte
}

// ProducePartition is the set of records destined for one partition.
type ProducePartition struct {
	PartitionID int32
	Records     []ProduceRecord
}

// ProduceTopic groups partitions under a topic name.
type ProduceTopic struct {
	Topic      string
	Partitions []ProducePartition
}

// ProduceRequest is API key 0.
//
// Wire layout (payload after frame header):
//
//	acks:           int16   (-1 = all ISR, 0 = none, 1 = leader)
//	timeout_ms:     int32
//	topic_count:    int32
//	  topic:        string
//	  part_count:   int32
//	    partition:  int32
//	    rec_count:  int32
//	      key_len:  int32  (-1 = null)
//	      key:      bytes
//	      val_len:  int32  (-1 = null)
//	      val:      bytes
type ProduceRequest struct {
	Acks      int16
	TimeoutMs int32
	Topics    []ProduceTopic
}

// DecodeProduceRequest parses a ProduceRequest from the raw payload bytes
// (everything after the frame header).
func DecodeProduceRequest(data []byte) (ProduceRequest, error) {
	var req ProduceRequest
	pos := 0

	if pos+6 > len(data) {
		return req, fmt.Errorf("produce request too short")
	}
	req.Acks = int16(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	req.TimeoutMs = int32(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	topicCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for t := int32(0); t < topicCount; t++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		partCount, newPos, err := DecodeInt32(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		pt := ProduceTopic{Topic: topic}
		for p := int32(0); p < partCount; p++ {
			partID, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			recCount, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			pp := ProducePartition{PartitionID: partID}
			for r := int32(0); r < recCount; r++ {
				keyLen, newPos, err := DecodeInt32(data, pos)
				if err != nil {
					return req, err
				}
				pos = newPos

				var key []byte
				if keyLen >= 0 {
					if pos+int(keyLen) > len(data) {
						return req, fmt.Errorf("key overflows buffer")
					}
					key = make([]byte, keyLen)
					copy(key, data[pos:pos+int(keyLen)])
					pos += int(keyLen)
				}

				valLen, newPos, err := DecodeInt32(data, pos)
				if err != nil {
					return req, err
				}
				pos = newPos

				var val []byte
				if valLen >= 0 {
					if pos+int(valLen) > len(data) {
						return req, fmt.Errorf("value overflows buffer")
					}
					val = make([]byte, valLen)
					copy(val, data[pos:pos+int(valLen)])
					pos += int(valLen)
				}

				pp.Records = append(pp.Records, ProduceRecord{Key: key, Value: val})
			}
			pt.Partitions = append(pt.Partitions, pp)
		}
		req.Topics = append(req.Topics, pt)
	}
	return req, nil
}

// ProducePartitionResult is the per-partition result in a ProduceResponse.
type ProducePartitionResult struct {
	PartitionID int32
	ErrorCode   ErrorCode
	BaseOffset  int64 // offset of the first record written
}

// ProduceTopicResult groups partition results under a topic.
type ProduceTopicResult struct {
	Topic      string
	Partitions []ProducePartitionResult
}

// ProduceResponse is the response to API key 0.
func EncodeProduceResponse(topics []ProduceTopicResult) []byte {
	var buf []byte
	buf = AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = AppendString(buf, t.Topic)
		buf = AppendInt32(buf, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			buf = AppendInt32(buf, p.PartitionID)
			buf = AppendInt16(buf, int16(p.ErrorCode))
			buf = AppendInt64(buf, p.BaseOffset)
		}
	}
	return buf
}

// ---- Fetch -----------------------------------------------------------------

// FetchPartition is one partition's fetch request.
type FetchPartition struct {
	PartitionID    int32
	FetchOffset    int64 // start reading from here
	MaxBytes       int32
}

// FetchTopic is one topic's worth of partition fetch requests.
type FetchTopic struct {
	Topic      string
	Partitions []FetchPartition
}

// FetchRequest is API key 1.
//
// Wire layout:
//
//	max_wait_ms:   int32
//	min_bytes:     int32
//	max_bytes:     int32
//	topic_count:   int32
//	  topic:       string
//	  part_count:  int32
//	    partition: int32
//	    offset:    int64
//	    max_bytes: int32
type FetchRequest struct {
	MaxWaitMs int32
	MinBytes  int32
	MaxBytes  int32
	Topics    []FetchTopic
}

func DecodeFetchRequest(data []byte) (FetchRequest, error) {
	var req FetchRequest
	pos := 0

	if pos+12 > len(data) {
		return req, fmt.Errorf("fetch request too short")
	}
	req.MaxWaitMs = int32(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	req.MinBytes = int32(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	req.MaxBytes = int32(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	topicCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for t := int32(0); t < topicCount; t++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		partCount, newPos, err := DecodeInt32(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		ft := FetchTopic{Topic: topic}
		for p := int32(0); p < partCount; p++ {
			partID, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			offset, newPos, err := DecodeInt64(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			maxBytes, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			ft.Partitions = append(ft.Partitions, FetchPartition{
				PartitionID: partID,
				FetchOffset: offset,
				MaxBytes:    maxBytes,
			})
		}
		req.Topics = append(req.Topics, ft)
	}
	return req, nil
}

// FetchRecord is one record in a FetchResponse.
type FetchRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// FetchPartitionResult is the per-partition result in a FetchResponse.
type FetchPartitionResult struct {
	PartitionID    int32
	ErrorCode      ErrorCode
	HighWatermark  int64
	Records        []FetchRecord
}

// FetchTopicResult groups partition results under a topic.
type FetchTopicResult struct {
	Topic      string
	Partitions []FetchPartitionResult
}

func EncodeFetchResponse(topics []FetchTopicResult) []byte {
	var buf []byte
	buf = AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = AppendString(buf, t.Topic)
		buf = AppendInt32(buf, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			buf = AppendInt32(buf, p.PartitionID)
			buf = AppendInt16(buf, int16(p.ErrorCode))
			buf = AppendInt64(buf, p.HighWatermark)
			buf = AppendInt32(buf, int32(len(p.Records)))
			for _, r := range p.Records {
				buf = AppendInt64(buf, r.Offset)
				buf = AppendInt64(buf, r.Timestamp)
				buf = AppendBytes(buf, r.Key)
				buf = AppendBytes(buf, r.Value)
			}
		}
	}
	return buf
}

// ---- Metadata --------------------------------------------------------------

// MetadataRequest is API key 2. Lists topics to describe; empty = all topics.
//
// Wire layout:
//
//	topic_count: int32
//	  topic:     string
type MetadataRequest struct {
	Topics []string // empty slice means "all topics"
}

func DecodeMetadataRequest(data []byte) (MetadataRequest, error) {
	var req MetadataRequest
	pos := 0

	count, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for i := int32(0); i < count; i++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos
		req.Topics = append(req.Topics, topic)
	}
	return req, nil
}

// BrokerInfo describes one broker in the cluster.
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

// PartitionMetadata describes one partition.
type PartitionMetadata struct {
	ErrorCode   ErrorCode
	PartitionID int32
	LeaderID    int32
	Replicas    []int32
	ISR         []int32
}

// TopicMetadata describes one topic.
type TopicMetadata struct {
	ErrorCode  ErrorCode
	Topic      string
	Partitions []PartitionMetadata
}

// MetadataResponse is the response to API key 2.
func EncodeMetadataResponse(brokers []BrokerInfo, topics []TopicMetadata) []byte {
	var buf []byte

	buf = AppendInt32(buf, int32(len(brokers)))
	for _, b := range brokers {
		buf = AppendInt32(buf, b.NodeID)
		buf = AppendString(buf, b.Host)
		buf = AppendInt32(buf, b.Port)
	}

	buf = AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = AppendInt16(buf, int16(t.ErrorCode))
		buf = AppendString(buf, t.Topic)
		buf = AppendInt32(buf, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			buf = AppendInt16(buf, int16(p.ErrorCode))
			buf = AppendInt32(buf, p.PartitionID)
			buf = AppendInt32(buf, p.LeaderID)
			buf = AppendInt32(buf, int32(len(p.Replicas)))
			for _, r := range p.Replicas {
				buf = AppendInt32(buf, r)
			}
			buf = AppendInt32(buf, int32(len(p.ISR)))
			for _, r := range p.ISR {
				buf = AppendInt32(buf, r)
			}
		}
	}
	return buf
}

// ---- OffsetCommit ----------------------------------------------------------

// OffsetCommitPartition is one partition in an OffsetCommit request.
type OffsetCommitPartition struct {
	PartitionID int32
	Offset      int64
	Metadata    string
}

// OffsetCommitTopic groups partitions under a topic.
type OffsetCommitTopic struct {
	Topic      string
	Partitions []OffsetCommitPartition
}

// OffsetCommitRequest is API key 6.
//
// Wire layout:
//
//	group_id:    string
//	topic_count: int32
//	  topic:     string
//	  part_count: int32
//	    partition: int32
//	    offset:    int64
//	    metadata:  string
type OffsetCommitRequest struct {
	GroupID string
	Topics  []OffsetCommitTopic
}

func DecodeOffsetCommitRequest(data []byte) (OffsetCommitRequest, error) {
	var req OffsetCommitRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	topicCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for t := int32(0); t < topicCount; t++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		partCount, newPos, err := DecodeInt32(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		ot := OffsetCommitTopic{Topic: topic}
		for p := int32(0); p < partCount; p++ {
			partID, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			offset, newPos, err := DecodeInt64(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			meta, newPos, err := DecodeString(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos

			ot.Partitions = append(ot.Partitions, OffsetCommitPartition{
				PartitionID: partID,
				Offset:      offset,
				Metadata:    meta,
			})
		}
		req.Topics = append(req.Topics, ot)
	}
	return req, nil
}

// OffsetCommitPartitionResult is the per-partition result.
type OffsetCommitPartitionResult struct {
	PartitionID int32
	ErrorCode   ErrorCode
}

// OffsetCommitTopicResult groups partition results.
type OffsetCommitTopicResult struct {
	Topic      string
	Partitions []OffsetCommitPartitionResult
}

func EncodeOffsetCommitResponse(topics []OffsetCommitTopicResult) []byte {
	var buf []byte
	buf = AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = AppendString(buf, t.Topic)
		buf = AppendInt32(buf, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			buf = AppendInt32(buf, p.PartitionID)
			buf = AppendInt16(buf, int16(p.ErrorCode))
		}
	}
	return buf
}

// ---- OffsetFetch -----------------------------------------------------------

// OffsetFetchPartition is one partition in an OffsetFetch request.
type OffsetFetchPartition struct {
	PartitionID int32
}

// OffsetFetchTopic groups partitions under a topic.
type OffsetFetchTopic struct {
	Topic      string
	Partitions []OffsetFetchPartition
}

// OffsetFetchRequest is API key 7.
//
// Wire layout:
//
//	group_id:    string
//	topic_count: int32
//	  topic:     string
//	  part_count: int32
//	    partition: int32
type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchTopic
}

func DecodeOffsetFetchRequest(data []byte) (OffsetFetchRequest, error) {
	var req OffsetFetchRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	topicCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for t := int32(0); t < topicCount; t++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		partCount, newPos, err := DecodeInt32(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		ft := OffsetFetchTopic{Topic: topic}
		for p := int32(0); p < partCount; p++ {
			partID, newPos, err := DecodeInt32(data, pos)
			if err != nil {
				return req, err
			}
			pos = newPos
			ft.Partitions = append(ft.Partitions, OffsetFetchPartition{PartitionID: partID})
		}
		req.Topics = append(req.Topics, ft)
	}
	return req, nil
}

// OffsetFetchPartitionResult is the per-partition result.
type OffsetFetchPartitionResult struct {
	PartitionID int32
	Offset      int64  // -1 if no committed offset
	Metadata    string
	ErrorCode   ErrorCode
}

// OffsetFetchTopicResult groups partition results.
type OffsetFetchTopicResult struct {
	Topic      string
	Partitions []OffsetFetchPartitionResult
}

func EncodeOffsetFetchResponse(topics []OffsetFetchTopicResult) []byte {
	var buf []byte
	buf = AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = AppendString(buf, t.Topic)
		buf = AppendInt32(buf, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			buf = AppendInt32(buf, p.PartitionID)
			buf = AppendInt64(buf, p.Offset)
			buf = AppendString(buf, p.Metadata)
			buf = AppendInt16(buf, int16(p.ErrorCode))
		}
	}
	return buf
}

// ---- CreateTopic -----------------------------------------------------------

// CreateTopicRequest is API key 10.
//
// Wire layout:
//
//	topic:             string
//	num_partitions:    int32
//	replication_factor: int32
type CreateTopicRequest struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int32
}

func DecodeCreateTopicRequest(data []byte) (CreateTopicRequest, error) {
	var req CreateTopicRequest
	pos := 0

	topic, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.Topic = topic
	pos = newPos

	numParts, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	req.NumPartitions = numParts
	pos = newPos

	rf, _, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	req.ReplicationFactor = rf

	return req, nil
}

// EncodeCreateTopicResponse encodes the response for CreateTopic.
func EncodeCreateTopicResponse(topic string, errCode ErrorCode) []byte {
	var buf []byte
	buf = AppendString(buf, topic)
	buf = AppendInt16(buf, int16(errCode))
	return buf
}
