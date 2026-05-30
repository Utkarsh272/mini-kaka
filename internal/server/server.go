package server

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
	"github.com/Utkarsh272/mini-kafka/internal/consumer_group"
	"github.com/Utkarsh272/mini-kafka/internal/protocol"
	"github.com/Utkarsh272/mini-kafka/internal/replication"
)

type Handler struct {
	broker *broker.Broker
}

func NewHandler(b *broker.Broker) *Handler {
	return &Handler{broker: b}
}

func (h *Handler) Handle(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	switch hdr.APIKey {
	case protocol.APIKeyProduce:
		h.handleProduce(w, hdr, payload)
	case protocol.APIKeyFetch:
		h.handleFetch(w, hdr, payload)
	case protocol.APIKeyMetadata:
		h.handleMetadata(w, hdr, payload)
	case protocol.APIKeyJoinGroup:
		h.handleJoinGroup(w, hdr, payload)
	case protocol.APIKeySyncGroup:
		h.handleSyncGroup(w, hdr, payload)
	case protocol.APIKeyHeartbeat:
		h.handleHeartbeat(w, hdr, payload)
	case protocol.APIKeyOffsetCommit:
		h.handleOffsetCommit(w, hdr, payload)
	case protocol.APIKeyOffsetFetch:
		h.handleOffsetFetch(w, hdr, payload)
	case protocol.APIKeyFetchFollower:
		h.handleFetchFollower(w, hdr, payload)
	case protocol.APIKeyLeaveGroup:
		h.handleLeaveGroup(w, hdr, payload)
	case protocol.APIKeyCreateTopic:
		h.handleCreateTopic(w, hdr, payload)
	case protocol.APIKeyDescribeGroup:
		h.handleDescribeGroup(w, hdr, payload)
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
			targetPartID := pp.PartitionID
			if targetPartID < 0 {
				routedResults := make(map[int32]*protocol.ProducePartitionResult)
				for _, rec := range pp.Records {
					partID, err := h.broker.RoutePartition(t.Topic, rec.Key)
					if err != nil {
						tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResult{
							PartitionID: -1, ErrorCode: protocol.ErrUnknownTopicPartition, BaseOffset: -1,
						})
						goto nextPartition
					}
					part, err := h.broker.GetPartition(t.Topic, partID)
					if err != nil {
						if _, seen := routedResults[partID]; !seen {
							routedResults[partID] = &protocol.ProducePartitionResult{
								PartitionID: partID, ErrorCode: protocol.ErrUnknownTopicPartition, BaseOffset: -1,
							}
						}
						continue
					}
					off, err := part.Append(rec.Key, rec.Value)
					if err != nil {
						slog.Error("append", "topic", t.Topic, "partition", partID, "err", err)
						if _, seen := routedResults[partID]; !seen {
							routedResults[partID] = &protocol.ProducePartitionResult{
								PartitionID: partID, ErrorCode: protocol.ErrUnknown, BaseOffset: -1,
							}
						}
						continue
					}
					if _, seen := routedResults[partID]; !seen {
						routedResults[partID] = &protocol.ProducePartitionResult{
							PartitionID: partID, ErrorCode: protocol.ErrNone, BaseOffset: off,
						}
					}
				}
				for _, res := range routedResults {
					tr.Partitions = append(tr.Partitions, *res)
				}
			} else {
				part, err := h.broker.GetPartition(t.Topic, targetPartID)
				if err != nil {
					tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResult{
						PartitionID: targetPartID, ErrorCode: protocol.ErrUnknownTopicPartition, BaseOffset: -1,
					})
					continue
				}
				var baseOffset int64 = -1
				var errCode protocol.ErrorCode
				for i, rec := range pp.Records {
					off, err := part.Append(rec.Key, rec.Value)
					if err != nil {
						slog.Error("append", "topic", t.Topic, "partition", targetPartID, "err", err)
						errCode = protocol.ErrUnknown
						break
					}
					if i == 0 {
						baseOffset = off
					}
				}
				tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResult{
					PartitionID: targetPartID, ErrorCode: errCode, BaseOffset: baseOffset,
				})
			}
		nextPartition:
		}
		topicResults = append(topicResults, tr)
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeProduceResponse(topicResults))
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
		maxBytes = 1 << 20
	}

	var topicResults []protocol.FetchTopicResult
	for _, t := range req.Topics {
		tr := protocol.FetchTopicResult{Topic: t.Topic}
		for _, fp := range t.Partitions {
			part, err := h.broker.GetPartition(t.Topic, fp.PartitionID)
			if err != nil {
				tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
					PartitionID: fp.PartitionID, ErrorCode: protocol.ErrUnknownTopicPartition, HighWatermark: -1,
				})
				continue
			}
			perPartMax := int64(maxBytes)
			if fp.MaxBytes > 0 {
				perPartMax = int64(fp.MaxBytes)
			}
			records, err := part.ReadBatch(fp.FetchOffset, perPartMax)
			if err != nil {
				slog.Warn("read batch", "topic", t.Topic, "partition", fp.PartitionID, "err", err)
				tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
					PartitionID: fp.PartitionID, ErrorCode: protocol.ErrOffsetOutOfRange,
					HighWatermark: part.HighWatermark(),
				})
				continue
			}
			var fetchRecords []protocol.FetchRecord
			for _, r := range records {
				fetchRecords = append(fetchRecords, protocol.FetchRecord{
					Offset: int64(r.Offset), Timestamp: int64(r.Timestamp),
					Key: r.Key, Value: r.Value,
				})
			}
			tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResult{
				PartitionID: fp.PartitionID, ErrorCode: protocol.ErrNone,
				HighWatermark: part.HighWatermark(), Records: fetchRecords,
			})
		}
		topicResults = append(topicResults, tr)
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeFetchResponse(topicResults))
}

// ---- FetchFollower (API key 8) ---------------------------------------------

// handleFetchFollower serves replication fetch requests from follower brokers.
// Returns records starting at the follower's current LEO plus the leader's LEO.
// ISR tracking via follower nodeID is a Day 13 (multi-broker) concern — the
// FetchFollower request payload does not yet carry a nodeID field.
func (h *Handler) handleFetchFollower(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := replication.DecodeFetchFollowerRequest(payload)
	if err != nil {
		slog.Error("decode fetch follower", "err", err)
		w.Write(replication.EncodeFetchFollowerResponse(hdr.CorrelationID,
			replication.FetchFollowerResponse{ErrorCode: -1}))
		return
	}

	part, err := h.broker.GetPartition(req.Topic, req.Partition)
	if err != nil {
		slog.Warn("fetch follower: unknown partition", "topic", req.Topic, "partition", req.Partition)
		w.Write(replication.EncodeFetchFollowerResponse(hdr.CorrelationID,
			replication.FetchFollowerResponse{ErrorCode: 3}))
		return
	}

	maxBytes := int64(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}

	records, err := part.ReadBatch(req.FromOffset, maxBytes)
	if err != nil {
		w.Write(replication.EncodeFetchFollowerResponse(hdr.CorrelationID,
			replication.FetchFollowerResponse{ErrorCode: 1, LeaderLEO: part.LogEndOffset()}))
		return
	}

	var fetchRecords []replication.FetchFollowerRecord
	for _, r := range records {
		fetchRecords = append(fetchRecords, replication.FetchFollowerRecord{
			Offset: int64(r.Offset), Timestamp: int64(r.Timestamp),
			Key: r.Key, Value: r.Value,
		})
	}

	w.Write(replication.EncodeFetchFollowerResponse(hdr.CorrelationID,
		replication.FetchFollowerResponse{
			ErrorCode: 0,
			LeaderLEO: part.LogEndOffset(),
			Records:   fetchRecords,
		}))
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
		NodeID: h.broker.NodeID(), Host: h.broker.Host(), Port: h.broker.Port(),
	}}

	allTopics := h.broker.ListTopics()
	var topicsToDescribe []string
	if len(req.Topics) == 0 {
		for _, t := range allTopics {
			topicsToDescribe = append(topicsToDescribe, t.Name())
		}
	} else {
		topicsToDescribe = req.Topics
	}

	topicMap := make(map[string]*broker.Topic, len(allTopics))
	for _, t := range allTopics {
		topicMap[t.Name()] = t
	}

	var topicMeta []protocol.TopicMetadata
	for _, name := range topicsToDescribe {
		t, ok := topicMap[name]
		if !ok {
			topicMeta = append(topicMeta, protocol.TopicMetadata{
				ErrorCode: protocol.ErrUnknownTopicPartition, Topic: name,
			})
			continue
		}
		tm := protocol.TopicMetadata{Topic: name}
		for i := 0; i < t.NumPartitions(); i++ {
			tm.Partitions = append(tm.Partitions, protocol.PartitionMetadata{
				PartitionID: int32(i), LeaderID: h.broker.NodeID(),
				Replicas: []int32{h.broker.NodeID()}, ISR: []int32{h.broker.NodeID()},
			})
		}
		topicMeta = append(topicMeta, tm)
	}

	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeMetadataResponse(brokerList, topicMeta))
}

// ---- JoinGroup -------------------------------------------------------------

func (h *Handler) handleJoinGroup(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeJoinGroupRequest(payload)
	if err != nil {
		slog.Error("decode join group", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	resp := h.broker.Coordinator.JoinGroup(consumer_group.JoinGroupRequest{
		GroupID: req.GroupID, SessionTimeout: req.SessionTimeout,
		MemberID: req.MemberID, ClientID: hdr.ClientID, Topics: req.Topics,
	})
	var members []protocol.JoinGroupMemberInfo
	for _, m := range resp.Members {
		members = append(members, protocol.JoinGroupMemberInfo{MemberID: m.MemberID, Topics: m.Topics})
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeJoinGroupResponse(resp.ErrorCode, resp.GenerationID, resp.GroupProtocol,
			resp.LeaderID, resp.MemberID, members))
}

// ---- SyncGroup -------------------------------------------------------------

func (h *Handler) handleSyncGroup(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeSyncGroupRequest(payload)
	if err != nil {
		slog.Error("decode sync group", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	var assignments []consumer_group.MemberAssignment
	for _, a := range req.Assignments {
		ma := consumer_group.MemberAssignment{MemberID: a.MemberID}
		for _, tpa := range a.Assignment {
			for _, p := range tpa.Partitions {
				ma.Partitions = append(ma.Partitions, consumer_group.TopicPartition{Topic: tpa.Topic, Partition: p})
			}
		}
		assignments = append(assignments, ma)
	}
	resp := h.broker.Coordinator.SyncGroup(consumer_group.SyncGroupRequest{
		GroupID: req.GroupID, GenerationID: req.GenerationID,
		MemberID: req.MemberID, Assignments: assignments,
	})
	topicMap := make(map[string][]int32)
	for _, tp := range resp.Assignment {
		topicMap[tp.Topic] = append(topicMap[tp.Topic], tp.Partition)
	}
	var wireAssignment []protocol.SyncGroupTopicAssignment
	for topic, parts := range topicMap {
		wireAssignment = append(wireAssignment, protocol.SyncGroupTopicAssignment{Topic: topic, Partitions: parts})
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeSyncGroupResponse(resp.ErrorCode, wireAssignment))
}

// ---- Heartbeat -------------------------------------------------------------

func (h *Handler) handleHeartbeat(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeHeartbeatRequest(payload)
	if err != nil {
		slog.Error("decode heartbeat", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	resp := h.broker.Coordinator.Heartbeat(consumer_group.HeartbeatRequest{
		GroupID: req.GroupID, GenerationID: req.GenerationID, MemberID: req.MemberID,
	})
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeHeartbeatResponse(resp.ErrorCode))
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
					PartitionID: p.PartitionID, ErrorCode: protocol.ErrUnknownTopicPartition,
				})
				continue
			}
			if err := h.broker.OffsetStore.Commit(req.GroupID, t.Topic, p.PartitionID, p.Offset); err != nil {
				slog.Error("persist offset", "err", err)
				tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResult{
					PartitionID: p.PartitionID, ErrorCode: protocol.ErrUnknown,
				})
				continue
			}
			part.CommitOffset(req.GroupID, p.Offset)
			tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResult{
				PartitionID: p.PartitionID, ErrorCode: protocol.ErrNone,
			})
		}
		topicResults = append(topicResults, tr)
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeOffsetCommitResponse(topicResults))
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
			if _, err := h.broker.GetPartition(t.Topic, p.PartitionID); err != nil {
				tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResult{
					PartitionID: p.PartitionID, Offset: -1, ErrorCode: protocol.ErrUnknownTopicPartition,
				})
				continue
			}
			committed := h.broker.OffsetStore.Fetch(req.GroupID, t.Topic, p.PartitionID)
			tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResult{
				PartitionID: p.PartitionID, Offset: committed, ErrorCode: protocol.ErrNone,
			})
		}
		topicResults = append(topicResults, tr)
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeOffsetFetchResponse(topicResults))
}

// ---- LeaveGroup ------------------------------------------------------------

func (h *Handler) handleLeaveGroup(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeLeaveGroupRequest(payload)
	if err != nil {
		slog.Error("decode leave group", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	resp := h.broker.Coordinator.LeaveGroup(consumer_group.LeaveGroupRequest{
		GroupID: req.GroupID, MemberID: req.MemberID,
	})
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeLeaveGroupResponse(resp.ErrorCode))
}

// ---- CreateTopic -----------------------------------------------------------

func (h *Handler) handleCreateTopic(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeCreateTopicRequest(payload)
	if err != nil {
		slog.Error("decode create topic", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	rf := req.ReplicationFactor
	if rf < 1 {
		rf = 1
	}
	err = h.broker.CreateTopic(req.Topic, req.NumPartitions, rf)
	var errCode protocol.ErrorCode
	if err != nil {
		slog.Error("create topic", "topic", req.Topic, "err", err)
		errCode = protocol.ErrTopicAlreadyExists
	} else {
		slog.Info("topic created", "topic", req.Topic, "partitions", req.NumPartitions, "rf", rf)
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeCreateTopicResponse(req.Topic, errCode))
}

// ---- DescribeGroup ---------------------------------------------------------

func (h *Handler) handleDescribeGroup(w io.Writer, hdr protocol.RequestHeader, payload []byte) {
	req, err := protocol.DecodeDescribeGroupRequest(payload)
	if err != nil {
		slog.Error("decode describe group", "err", err)
		protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrInvalidRequest, nil)
		return
	}
	desc := h.broker.Coordinator.DescribeGroup(req.GroupID)
	var members []protocol.DescribeGroupMember
	for _, m := range desc.Members {
		topicMap := make(map[string][]int32)
		for _, tp := range m.Assignment {
			topicMap[tp.Topic] = append(topicMap[tp.Topic], tp.Partition)
		}
		var assignment []protocol.TopicPartitionAssignment
		for topic, parts := range topicMap {
			assignment = append(assignment, protocol.TopicPartitionAssignment{Topic: topic, Partitions: parts})
		}
		members = append(members, protocol.DescribeGroupMember{
			MemberID: m.MemberID, ClientID: m.ClientID, Assignment: assignment,
		})
	}
	protocol.WriteResponse(w, hdr.CorrelationID, protocol.ErrNone,
		protocol.EncodeDescribeGroupResponse(desc.ErrorCode, desc.GroupID, desc.State,
			desc.LeaderID, desc.GenerationID, members))
}

// ---- TCP Server ------------------------------------------------------------

type Server struct {
	addr     string
	handler  *Handler
	listener net.Listener
	wg       sync.WaitGroup
}

func NewServer(addr string, h *Handler) *Server {
	return &Server{addr: addr, handler: h}
}

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
			return nil
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.serveConn(c)
		}(conn)
	}
}

func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

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
