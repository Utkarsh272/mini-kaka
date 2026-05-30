package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/storage"
)

const (
	// fetchFollowerAPIKey is the wire API key for FetchFollower requests.
	fetchFollowerAPIKey byte = 8

	// fetchInterval is how often the follower polls the leader when caught up.
	fetchInterval = 100 * time.Millisecond

	// fetchMaxBytes is the maximum bytes fetched per round from the leader.
	fetchMaxBytes = int32(1 << 20) // 1 MB

	// dialTimeout is how long to wait when connecting to the leader.
	dialTimeout = 5 * time.Second

	// reconnectDelay is how long to wait before retrying after a lost connection.
	reconnectDelay = 1 * time.Second
)

// FetchFollowerRequest is the wire payload for API key 8.
// The follower sends its current logEndOffset as the fetch position.
//
// Wire layout:
//
//	topic:      string (2B len + bytes)
//	partition:  int32
//	fromOffset: int64
//	maxBytes:   int32
type FetchFollowerRequest struct {
	Topic      string
	Partition  int32
	FromOffset int64
	MaxBytes   int32
}

// FetchFollowerRecord is one record in a FetchFollower response.
type FetchFollowerRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// FetchFollowerResponse is the wire response for API key 8.
//
// Wire layout:
//
//	error_code:    int16
//	leader_leo:    int64  (leader's logEndOffset — used to detect lag)
//	record_count:  int32
//	  offset:      int64
//	  timestamp:   int64
//	  key_len:     int32 (-1 = null)
//	  key:         bytes
//	  val_len:     int32 (-1 = null)
//	  val:         bytes
type FetchFollowerResponse struct {
	ErrorCode  int16
	LeaderLEO  int64
	Records    []FetchFollowerRecord
}

// EncodeFetchFollowerRequest serialises a FetchFollower request into the full
// wire frame (length prefix + header + payload).
func EncodeFetchFollowerRequest(corrID uint32, req FetchFollowerRequest) []byte {
	// Payload: topic(2+N) + partition(4) + fromOffset(8) + maxBytes(4)
	topicBytes := []byte(req.Topic)
	payloadLen := 2 + len(topicBytes) + 4 + 8 + 4

	// Frame header: api_key(1) + corr_id(4) + client_id_len(2) = 7 bytes
	// (client_id is empty = 0 bytes)
	headerLen := 7
	totalLen := headerLen + payloadLen

	buf := make([]byte, 4+totalLen)
	pos := 0

	// length field (does not include itself)
	binary.BigEndian.PutUint32(buf[pos:], uint32(totalLen))
	pos += 4

	// api_key
	buf[pos] = fetchFollowerAPIKey
	pos++

	// correlation_id
	binary.BigEndian.PutUint32(buf[pos:], corrID)
	pos += 4

	// client_id_len = 0
	binary.BigEndian.PutUint16(buf[pos:], 0)
	pos += 2

	// payload: topic
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:], topicBytes)
	pos += len(topicBytes)

	// partition
	binary.BigEndian.PutUint32(buf[pos:], uint32(req.Partition))
	pos += 4

	// fromOffset
	binary.BigEndian.PutUint64(buf[pos:], uint64(req.FromOffset))
	pos += 8

	// maxBytes
	binary.BigEndian.PutUint32(buf[pos:], uint32(req.MaxBytes))

	return buf
}

// DecodeFetchFollowerRequest parses the payload bytes (after the frame header)
// into a FetchFollowerRequest.
func DecodeFetchFollowerRequest(data []byte) (FetchFollowerRequest, error) {
	var req FetchFollowerRequest
	pos := 0

	if pos+2 > len(data) {
		return req, fmt.Errorf("truncated topic len")
	}
	topicLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	if pos+topicLen > len(data) {
		return req, fmt.Errorf("truncated topic")
	}
	req.Topic = string(data[pos : pos+topicLen])
	pos += topicLen

	if pos+4 > len(data) {
		return req, fmt.Errorf("truncated partition")
	}
	req.Partition = int32(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	if pos+8 > len(data) {
		return req, fmt.Errorf("truncated fromOffset")
	}
	req.FromOffset = int64(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	if pos+4 > len(data) {
		return req, fmt.Errorf("truncated maxBytes")
	}
	req.MaxBytes = int32(binary.BigEndian.Uint32(data[pos:]))

	return req, nil
}

// EncodeFetchFollowerResponse serialises a FetchFollowerResponse into a wire
// frame (length prefix + corr_id + error_code + payload).
func EncodeFetchFollowerResponse(corrID uint32, resp FetchFollowerResponse) []byte {
	// Payload: error_code(2) + leader_leo(8) + record_count(4) + records
	payloadSize := 2 + 8 + 4
	for _, r := range resp.Records {
		payloadSize += 8 + 8 // offset + timestamp
		payloadSize += 4     // key_len
		if r.Key != nil {
			payloadSize += len(r.Key)
		}
		payloadSize += 4 // val_len
		if r.Value != nil {
			payloadSize += len(r.Value)
		}
	}

	// Response frame: corr_id(4) + error_code(2) + payload
	// But our frame.go WriteResponse puts error_code in the fixed header.
	// For FetchFollower we embed the replication error_code inside the payload
	// and use ErrNone (0) in the outer frame header.
	framePayload := make([]byte, payloadSize)
	pos := 0

	binary.BigEndian.PutUint16(framePayload[pos:], uint16(resp.ErrorCode))
	pos += 2
	binary.BigEndian.PutUint64(framePayload[pos:], uint64(resp.LeaderLEO))
	pos += 8
	binary.BigEndian.PutUint32(framePayload[pos:], uint32(len(resp.Records)))
	pos += 4

	for _, r := range resp.Records {
		binary.BigEndian.PutUint64(framePayload[pos:], uint64(r.Offset))
		pos += 8
		binary.BigEndian.PutUint64(framePayload[pos:], uint64(r.Timestamp))
		pos += 8

		if r.Key == nil {
			binary.BigEndian.PutUint32(framePayload[pos:], uint32(0xFFFFFFFF)) // -1
		} else {
			binary.BigEndian.PutUint32(framePayload[pos:], uint32(len(r.Key)))
		}
		pos += 4
		if r.Key != nil {
			copy(framePayload[pos:], r.Key)
			pos += len(r.Key)
		}

		if r.Value == nil {
			binary.BigEndian.PutUint32(framePayload[pos:], uint32(0xFFFFFFFF))
		} else {
			binary.BigEndian.PutUint32(framePayload[pos:], uint32(len(r.Value)))
		}
		pos += 4
		if r.Value != nil {
			copy(framePayload[pos:], r.Value)
			pos += len(r.Value)
		}
	}

	// Outer frame: [totalLen: 4B][corrID: 4B][outerErrCode: 2B][payload]
	total := 4 + 2 + len(framePayload)
	frame := make([]byte, 4+total)
	binary.BigEndian.PutUint32(frame[0:], uint32(total))
	binary.BigEndian.PutUint32(frame[4:], corrID)
	binary.BigEndian.PutUint16(frame[8:], 0) // outer error = none
	copy(frame[10:], framePayload)
	return frame
}

// DecodeFetchFollowerResponse parses the payload portion of a FetchFollower
// response (after the outer frame header has been stripped).
func DecodeFetchFollowerResponse(payload []byte) (FetchFollowerResponse, error) {
	var resp FetchFollowerResponse
	pos := 0

	if pos+2 > len(payload) {
		return resp, fmt.Errorf("truncated error_code")
	}
	resp.ErrorCode = int16(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2

	if pos+8 > len(payload) {
		return resp, fmt.Errorf("truncated leader_leo")
	}
	resp.LeaderLEO = int64(binary.BigEndian.Uint64(payload[pos:]))
	pos += 8

	if pos+4 > len(payload) {
		return resp, fmt.Errorf("truncated record_count")
	}
	recCount := int(binary.BigEndian.Uint32(payload[pos:]))
	pos += 4

	for i := 0; i < recCount; i++ {
		if pos+8 > len(payload) {
			return resp, fmt.Errorf("truncated offset")
		}
		offset := int64(binary.BigEndian.Uint64(payload[pos:]))
		pos += 8

		if pos+8 > len(payload) {
			return resp, fmt.Errorf("truncated timestamp")
		}
		ts := int64(binary.BigEndian.Uint64(payload[pos:]))
		pos += 8

		if pos+4 > len(payload) {
			return resp, fmt.Errorf("truncated key_len")
		}
		keyLen := int32(binary.BigEndian.Uint32(payload[pos:]))
		pos += 4

		var key []byte
		if keyLen >= 0 {
			if pos+int(keyLen) > len(payload) {
				return resp, fmt.Errorf("truncated key")
			}
			key = make([]byte, keyLen)
			copy(key, payload[pos:pos+int(keyLen)])
			pos += int(keyLen)
		}

		if pos+4 > len(payload) {
			return resp, fmt.Errorf("truncated val_len")
		}
		valLen := int32(binary.BigEndian.Uint32(payload[pos:]))
		pos += 4

		var val []byte
		if valLen >= 0 {
			if pos+int(valLen) > len(payload) {
				return resp, fmt.Errorf("truncated value")
			}
			val = make([]byte, valLen)
			copy(val, payload[pos:pos+int(valLen)])
			pos += int(valLen)
		}

		resp.Records = append(resp.Records, FetchFollowerRecord{
			Offset: offset, Timestamp: ts, Key: key, Value: val,
		})
	}

	return resp, nil
}

// FollowerFetcher is a background goroutine that runs on a follower broker.
// It maintains a persistent TCP connection to the leader and continuously
// fetches records, appending them to the local partition log.
//
// When it receives a batch it also reports the new fetch offset back to the
// leader so the leader can advance the high-watermark.
type FollowerFetcher struct {
	// Identity
	followerNodeID int32
	leaderAddr     string // "host:port"
	topic          string
	partition      int32

	// Local log to append fetched records into.
	log *storage.Log

	// onFetch is called after each successful fetch with the new LEO.
	// Used to report progress back to the ISR tracker on the leader side
	// when running in-process tests; in a real multi-broker setup the leader
	// learns the follower's offset from the FetchFollower request itself.
	onFetch func(fetchedUpTo int64)

	// stop signals the fetcher to shut down.
	stop chan struct{}
	done chan struct{}
}

// NewFollowerFetcher creates a FollowerFetcher. Call Start() to begin fetching.
func NewFollowerFetcher(
	followerNodeID int32,
	leaderAddr string,
	topic string,
	partition int32,
	log *storage.Log,
	onFetch func(fetchedUpTo int64),
) *FollowerFetcher {
	return &FollowerFetcher{
		followerNodeID: followerNodeID,
		leaderAddr:     leaderAddr,
		topic:          topic,
		partition:      partition,
		log:            log,
		onFetch:        onFetch,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
}

// Start launches the fetch loop in a background goroutine.
func (f *FollowerFetcher) Start() {
	go f.run()
}

// Stop signals the fetcher to stop and waits for it to exit.
func (f *FollowerFetcher) Stop() {
	close(f.stop)
	<-f.done
}

// run is the main fetch loop. It reconnects on error and backs off before
// retrying so a downed leader doesn't busy-loop.
func (f *FollowerFetcher) run() {
	defer close(f.done)

	var corrID uint32

	for {
		select {
		case <-f.stop:
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", f.leaderAddr, dialTimeout)
		if err != nil {
			slog.Warn("follower: dial leader failed",
				"leader", f.leaderAddr, "topic", f.topic,
				"partition", f.partition, "err", err)
			select {
			case <-f.stop:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		slog.Info("follower: connected to leader",
			"leader", f.leaderAddr, "topic", f.topic, "partition", f.partition)

		if err := f.fetchLoop(conn, &corrID); err != nil {
			if err != io.EOF {
				slog.Warn("follower: fetch loop error",
					"leader", f.leaderAddr, "err", err)
			}
			conn.Close()
			select {
			case <-f.stop:
				return
			case <-time.After(reconnectDelay):
			}
		}
	}
}

// fetchLoop runs the continuous fetch-append cycle over a single connection.
func (f *FollowerFetcher) fetchLoop(conn net.Conn, corrID *uint32) error {
	for {
		select {
		case <-f.stop:
			return nil
		default:
		}

		fromOffset := int64(f.log.LogEndOffset())

		*corrID++
		req := FetchFollowerRequest{
			Topic:      f.topic,
			Partition:  f.partition,
			FromOffset: fromOffset,
			MaxBytes:   fetchMaxBytes,
		}

		frame := EncodeFetchFollowerRequest(*corrID, req)
		if _, err := conn.Write(frame); err != nil {
			return fmt.Errorf("write fetch request: %w", err)
		}

		resp, err := readFetchFollowerResponse(conn)
		if err != nil {
			return fmt.Errorf("read fetch response: %w", err)
		}

		if resp.ErrorCode != 0 {
			slog.Warn("follower: leader returned error",
				"error_code", resp.ErrorCode,
				"topic", f.topic, "partition", f.partition)
			time.Sleep(fetchInterval)
			continue
		}

		// Append received records to local log.
		for _, rec := range resp.Records {
			if _, err := f.log.Append(rec.Key, rec.Value); err != nil {
				return fmt.Errorf("append record offset %d: %w", rec.Offset, err)
			}
		}

		newLEO := int64(f.log.LogEndOffset())
		if f.onFetch != nil {
			f.onFetch(newLEO)
		}

		// If we received no records we're caught up — back off briefly.
		if len(resp.Records) == 0 {
			select {
			case <-f.stop:
				return nil
			case <-time.After(fetchInterval):
			}
		}
	}
}

// readFetchFollowerResponse reads one complete FetchFollower response frame
// from conn and returns the decoded response.
func readFetchFollowerResponse(r io.Reader) (FetchFollowerResponse, error) {
	// Read 4-byte total length.
	var totalLen uint32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return FetchFollowerResponse{}, err
	}

	body := make([]byte, totalLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return FetchFollowerResponse{}, fmt.Errorf("read response body: %w", err)
	}

	// body: corr_id(4) + outer_error_code(2) + payload
	if len(body) < 6 {
		return FetchFollowerResponse{}, fmt.Errorf("response too short")
	}
	payload := body[6:] // skip corr_id + outer error code

	return DecodeFetchFollowerResponse(payload)
}
