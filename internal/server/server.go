package server

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

// Handler dispatches incoming requests to the right broker operation and
// encodes the response. One Handler is shared across all connections.
type Handler struct {
	broker *broker.Broker
}

// NewHandler creates a Handler backed by the given Broker.
func NewHandler(b *broker.Broker) *Handler {
	return &Handler{broker: b}
}

// Handle processes a single decoded request and writes the response to w.
// w is a buffered writer; the caller flushes after Handle returns.
func (h *Handler) Handle(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	switch hdr.APIKey {
	case protocol.APIKeyProduce:
		h.handleProduce(w, hdr, payload)
	case protocol.APIKeyFetch:
		h.handleFetch(w, hdr, payload)
	case protocol.APIKeyMetadata:
		h.handleMetadata(w, hdr, payload)
	case protocol.APIKeyOffsetCommit:
		h.handleOffsetCommit(w, hdr, payload)
	case protocol.APIKeyOffsetFetch:
		h.handleOffsetFetch(w, hdr, payload)
	case protocol.APIKeyCreateTopic:
		h.handleCreateTopic(w, hdr, payload)
	default:
		slog.Warn("unknown api_key", "api_key", hdr.APIKey, "client", hdr.ClientID)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrUnknown, nil)
	}
}

// ---- Produce ---------------------------------------------------------------

func (h *Handler) handleProduce(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeProduceRequest(payload)
	if err != nil {
		slog.Error("decode produce", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	var topicResults []protocol.ProduceTopicResult
	for _, t := range req.Topics {
		tr := protocol.ProduceTopicResult{Topic: t.Topic}
		for _, pp := range t.Partitions {
			part, err := h.broker.GetPartition(t.Topic, pp.PartitionID)
			if err != nil {
				tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResult{
					PartitionID: pp.PartitionID,
					ErrorCode:   protocol.ErrUnknownTopicPartition,
					BaseOffset:  -1,
				})
				continue
			}

			var baseOffset int64 = -1
			var errCode protocol.ErrorCode
			for i, rec := range pp.Records {
				off, err := part.Append(rec.Key, rec.Value)
				if err != nil {
					slog.Error("append record", "topic", t.Topic, "partition", pp.PartitionID, "err", err)
					errCode = protocol.ErrUnknown
					break
				}
				if i == 0 {
					baseOffset = off
				}
			}
			tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResult{
				PartitionID: pp.PartitionID,
				ErrorCode:   errCode,
				BaseOffset:  baseOffset,
			})
		}
		topicResults = append(topicResults, tr)
	}

	respPayload := protocol.EncodeProduceResponse(topicResults)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- Fetch -----------------------------------------------------------------

func (h *Handler) handleFetch(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeFetchRequest(payload)
	if err != nil {
		slog.Error("decode fetch", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1 MB default
	}

	var topicResults []protocol.FetchTopicResult
	for _, t := range req.Topics {
		tr := protocol.FetchTopicResult{Topic: t.Topic}
		for _, fp := range t.Partitions {
			part, err := h.broker.GetPartition(t.Topic, fp.PartitionID)
			if err != nil {
				tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
					PartitionID:   fp.PartitionID,
					ErrorCode:     protocol.ErrUnknownTopicPartition,
					HighWatermark: -1,
				})
				continue
			}

			perPartMax := int64(maxBytes)
			if fp.MaxBytes > 0 {
				perPartMax = int64(fp.MaxBytes)
			}

			records, err := part.ReadBatch(fp.FetchOffset, perPartMax)
			if err != nil {
				slog.Warn("read batch", "topic", t.Topic, "partition", fp.PartitionID,
					"offset", fp.FetchOffset, "err", err)
				tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
					PartitionID:   fp.PartitionID,
					ErrorCode:     protocol.ErrOffsetOutOfRange,
					HighWatermark: part.HighWatermark(),
				})
				continue
			}

			var fetchRecords []protocol.FetchRecord
			for _, r := range records {
				fetchRecords = append(fetchRecords, protocol.FetchRecord{
					Offset:    int64(r.Offset),
					Timestamp: int64(r.Timestamp),
					Key:       r.Key,
					Value:     r.Value,
				})
			}

			tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
				PartitionID:   fp.PartitionID,
				ErrorCode:     protocol.ErrNone,
				HighWatermark: part.HighWatermark(),
				Records:       fetchRecords,
			})
		}
		topicResults = append(topicResults, tr)
	}

	respPayload := protocol.EncodeFetchResponse(topicResults)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- Metadata --------------------------------------------------------------

func (h *Handler) handleMetadata(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeMetadataRequest(payload)
	if err != nil {
		slog.Error("decode metadata", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	brokerList := []protocol.BrokerInfo{{
		NodeID: h.broker.NodeID(),
		Host:   h.broker.Host(),
		Port:   h.broker.Port(),
	}}

	// Collect topic names to describe: empty request means all topics.
	allTopics := h.broker.ListTopics()
	var topicsToDescribe []string
	if len(req.Topics) == 0 {
		for _, t := range allTopics {
			topicsToDescribe = append(topicsToDescribe, t.Name())
		}
	} else {
		topicsToDescribe = req.Topics
	}

	// Build a name → *Topic map for O(1) lookup.
	topicMap := make(map[string]*broker.Topic, len(allTopics))
	for _, t := range allTopics {
		topicMap[t.Name()] = t
	}

	var topicMeta []protocol.TopicMetadata
	for _, name := range topicsToDescribe {
		t, ok := topicMap[name]
		if !ok {
			topicMeta = append(topicMeta, protocol.TopicMetadata{
				ErrorCode: protocol.ErrUnknownTopicPartition,
				Topic:     name,
			})
			continue
		}

		tm := protocol.TopicMetadata{Topic: name}
		for i := 0; i < t.NumPartitions(); i++ {
			tm.Partitions = append(tm.Partitions, protocol.PartitionMetadata{
				PartitionID: int32(i),
				LeaderID:    h.broker.NodeID(),
				Replicas:    []int32{h.broker.NodeID()},
				ISR:         []int32{h.broker.NodeID()},
			})
		}
		topicMeta = append(topicMeta, tm)
	}

	respPayload := protocol.EncodeMetadataResponse(brokerList, topicMeta)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- OffsetCommit ----------------------------------------------------------

func (h *Handler) handleOffsetCommit(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeOffsetCommitRequest(payload)
	if err != nil {
		slog.Error("decode offset commit", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	var topicResults []protocol.OffsetCommitTopicResult
	for _, t := range req.Topics {
		tr := protocol.OffsetCommitTopicResult{Topic: t.Topic}
		for _, p := range t.Partitions {
			part, err := h.broker.GetPartition(t.Topic, p.PartitionID)
			if err != nil {
				tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResult{
					PartitionID: p.PartitionID,
					ErrorCode:   protocol.ErrUnknownTopicPartition,
				})
				continue
			}
			part.CommitOffset(req.GroupID, p.Offset)
			tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResult{
				PartitionID: p.PartitionID,
				ErrorCode:   protocol.ErrNone,
			})
		}
		topicResults = append(topicResults, tr)
	}

	respPayload := protocol.EncodeOffsetCommitResponse(topicResults)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- OffsetFetch -----------------------------------------------------------

func (h *Handler) handleOffsetFetch(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeOffsetFetchRequest(payload)
	if err != nil {
		slog.Error("decode offset fetch", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	var topicResults []protocol.OffsetFetchTopicResult
	for _, t := range req.Topics {
		tr := protocol.OffsetFetchTopicResult{Topic: t.Topic}
		for _, p := range t.Partitions {
			part, err := h.broker.GetPartition(t.Topic, p.PartitionID)
			if err != nil {
				tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResult{
					PartitionID: p.PartitionID,
					Offset:      -1,
					ErrorCode:   protocol.ErrUnknownTopicPartition,
				})
				continue
			}
			committed := part.FetchOffset(req.GroupID)
			tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResult{
				PartitionID: p.PartitionID,
				Offset:      committed,
				ErrorCode:   protocol.ErrNone,
			})
		}
		topicResults = append(topicResults, tr)
	}

	respPayload := protocol.EncodeOffsetFetchResponse(topicResults)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- CreateTopic -----------------------------------------------------------

func (h *Handler) handleCreateTopic(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeCreateTopicRequest(payload)
	if err != nil {
		slog.Error("decode create topic", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}

	err = h.broker.CreateTopic(req.Topic, req.NumPartitions)
	var errCode protocol.ErrorCode
	if err != nil {
		slog.Error("create topic", "topic", req.Topic, "err", err)
		errCode = protocol.ErrTopicAlreadyExists
	} else {
		slog.Info("topic created", "topic", req.Topic, "partitions", req.NumPartitions)
	}

	respPayload := protocol.EncodeCreateTopicResponse(req.Topic, errCode)
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone, respPayload)
}

// ---- TCP Server ------------------------------------------------------------

// Server listens for TCP connections and dispatches each to its own goroutine.
type Server struct {
	addr     string
	handler  *Handler
	listener net.Listener
	wg       sync.WaitGroup
}

// NewServer creates a Server that will listen on addr.
func NewServer(addr string, h *Handler) *Server {
	return &Server{addr: addr, handler: h}
}

// ListenAndServe starts the TCP listener and accepts connections until Close
// is called.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.listener = ln
	slog.Info("broker listening", "addr", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil // listener closed — normal shutdown
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.serveConn(c)
		}(conn)
	}
}

// Close stops accepting new connections and waits for active ones to finish.
func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// serveConn handles one client connection for its lifetime. Requests are
// processed sequentially; each response is written and flushed before reading
// the next request. The correlation ID allows the client to match responses.
func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	slog.Info("client connected", "remote", remote)
	defer slog.Info("client disconnected", "remote", remote)

	br := bufio.NewReaderSize(conn, 64*1024)
	bw := bufio.NewWriterSize(conn, 64*1024)

	for {
		hdr, payload, err := protocol.ReadRequest(br)
		if err != nil {
			if err != io.EOF {
				slog.Warn("read request", "remote", remote, "err", err)
			}
			return
		}

		s.handler.Handle(bw, hdr, payload)

		if err := bw.Flush(); err != nil {
			slog.Warn("flush response", "remote", remote, "err", err)
			return
		}
	}
}
