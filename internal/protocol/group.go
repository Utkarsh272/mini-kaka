package protocol

import "fmt"

// ---- JoinGroup (API key 3) -------------------------------------------------

// JoinGroupRequest is sent by a consumer when joining or rejoining a group.
//
// Wire layout:
//
//	group_id:        string
//	session_timeout: int32  (ms)
//	member_id:       string (empty = new member)
//	topic_count:     int32
//	  topic:         string
type JoinGroupRequest struct {
	GroupID        string
	SessionTimeout int32
	MemberID       string
	Topics         []string
}

func DecodeJoinGroupRequest(data []byte) (JoinGroupRequest, error) {
	var req JoinGroupRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	sessionTimeout, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	req.SessionTimeout = sessionTimeout
	pos = newPos

	memberID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.MemberID = memberID
	pos = newPos

	topicCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for i := int32(0); i < topicCount; i++ {
		topic, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos
		req.Topics = append(req.Topics, topic)
	}
	return req, nil
}

// JoinGroupMemberInfo is per-member metadata sent to the group leader.
type JoinGroupMemberInfo struct {
	MemberID string
	Topics   []string
}

// JoinGroupResponse is returned to each consumer after the rebalance phase.
//
// Wire layout:
//
//	error_code:     int16
//	generation_id:  int32
//	protocol:       string
//	leader_id:      string
//	member_id:      string
//	member_count:   int32    (only > 0 for the leader)
//	  member_id:    string
//	  topic_count:  int32
//	    topic:      string
func EncodeJoinGroupResponse(
	errCode int16,
	generationID int32,
	protocol, leaderID, memberID string,
	members []JoinGroupMemberInfo,
) []byte {
	var buf []byte
	buf = AppendInt16(buf, errCode)
	buf = AppendInt32(buf, generationID)
	buf = AppendString(buf, protocol)
	buf = AppendString(buf, leaderID)
	buf = AppendString(buf, memberID)
	buf = AppendInt32(buf, int32(len(members)))
	for _, m := range members {
		buf = AppendString(buf, m.MemberID)
		buf = AppendInt32(buf, int32(len(m.Topics)))
		for _, t := range m.Topics {
			buf = AppendString(buf, t)
		}
	}
	return buf
}

// ---- SyncGroup (API key 4) -------------------------------------------------

// TopicPartitionAssignment is one topic + its partitions in a SyncGroup payload.
type TopicPartitionAssignment struct {
	Topic      string
	Partitions []int32
}

// MemberAssignmentWire is one member's full assignment in a SyncGroup request.
type MemberAssignmentWire struct {
	MemberID   string
	Assignment []TopicPartitionAssignment
}

// SyncGroupRequest is sent by all members after JoinGroup completes.
// Only the leader sends a non-empty Assignments list.
//
// Wire layout:
//
//	group_id:       string
//	generation_id:  int32
//	member_id:      string
//	assign_count:   int32
//	  member_id:    string
//	  topic_count:  int32
//	    topic:      string
//	    part_count: int32
//	      partition: int32
type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []MemberAssignmentWire
}

func DecodeSyncGroupRequest(data []byte) (SyncGroupRequest, error) {
	var req SyncGroupRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	genID, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	req.GenerationID = genID
	pos = newPos

	memberID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.MemberID = memberID
	pos = newPos

	assignCount, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	pos = newPos

	for a := int32(0); a < assignCount; a++ {
		mID, newPos, err := DecodeString(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		topicCount, newPos, err := DecodeInt32(data, pos)
		if err != nil {
			return req, err
		}
		pos = newPos

		maw := MemberAssignmentWire{MemberID: mID}
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

			tpa := TopicPartitionAssignment{Topic: topic}
			for p := int32(0); p < partCount; p++ {
				partID, newPos, err := DecodeInt32(data, pos)
				if err != nil {
					return req, err
				}
				pos = newPos
				tpa.Partitions = append(tpa.Partitions, partID)
			}
			maw.Assignment = append(maw.Assignment, tpa)
		}
		req.Assignments = append(req.Assignments, maw)
	}
	return req, nil
}

// SyncGroupTopicAssignment is one topic's partitions in the response.
type SyncGroupTopicAssignment struct {
	Topic      string
	Partitions []int32
}

// EncodeSyncGroupResponse encodes the per-member partition assignment.
//
// Wire layout:
//
//	error_code:    int16
//	topic_count:   int32
//	  topic:       string
//	  part_count:  int32
//	    partition: int32
func EncodeSyncGroupResponse(errCode int16, assignment []SyncGroupTopicAssignment) []byte {
	var buf []byte
	buf = AppendInt16(buf, errCode)
	buf = AppendInt32(buf, int32(len(assignment)))
	for _, a := range assignment {
		buf = AppendString(buf, a.Topic)
		buf = AppendInt32(buf, int32(len(a.Partitions)))
		for _, p := range a.Partitions {
			buf = AppendInt32(buf, p)
		}
	}
	return buf
}

// ---- Heartbeat (API key 5) -------------------------------------------------

// HeartbeatRequest is sent periodically by consumers to stay in the group.
//
// Wire layout:
//
//	group_id:      string
//	generation_id: int32
//	member_id:     string
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func DecodeHeartbeatRequest(data []byte) (HeartbeatRequest, error) {
	var req HeartbeatRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	genID, newPos, err := DecodeInt32(data, pos)
	if err != nil {
		return req, err
	}
	req.GenerationID = genID
	pos = newPos

	memberID, _, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.MemberID = memberID
	return req, nil
}

// EncodeHeartbeatResponse encodes a Heartbeat response.
func EncodeHeartbeatResponse(errCode int16) []byte {
	return AppendInt16(nil, errCode)
}

// ---- LeaveGroup (API key 9) ------------------------------------------------

// LeaveGroupRequest is sent when a consumer shuts down cleanly.
//
// Wire layout:
//
//	group_id:  string
//	member_id: string
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func DecodeLeaveGroupRequest(data []byte) (LeaveGroupRequest, error) {
	var req LeaveGroupRequest
	pos := 0

	groupID, newPos, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.GroupID = groupID
	pos = newPos

	memberID, _, err := DecodeString(data, pos)
	if err != nil {
		return req, err
	}
	req.MemberID = memberID
	return req, nil
}

// EncodeLeaveGroupResponse encodes a LeaveGroup response.
func EncodeLeaveGroupResponse(errCode int16) []byte {
	return AppendInt16(nil, errCode)
}

// ---- DescribeGroup (API key 11) --------------------------------------------

// DescribeGroupRequest asks for the state of a consumer group.
//
// Wire layout:
//
//	group_id: string
type DescribeGroupRequest struct {
	GroupID string
}

func DecodeDescribeGroupRequest(data []byte) (DescribeGroupRequest, error) {
	groupID, _, err := DecodeString(data, 0)
	if err != nil {
		return DescribeGroupRequest{}, fmt.Errorf("decode group_id: %w", err)
	}
	return DescribeGroupRequest{GroupID: groupID}, nil
}

// DescribeGroupMember is one member's info in a DescribeGroup response.
type DescribeGroupMember struct {
	MemberID   string
	ClientID   string
	Assignment []TopicPartitionAssignment
}

// EncodeDescribeGroupResponse encodes the group description.
//
// Wire layout:
//
//	error_code:    int16
//	group_id:      string
//	state:         string
//	generation_id: int32
//	leader_id:     string
//	member_count:  int32
//	  member_id:   string
//	  client_id:   string
//	  topic_count: int32
//	    topic:     string
//	    part_count:int32
//	      partition:int32
func EncodeDescribeGroupResponse(
	errCode int16,
	groupID, state, leaderID string,
	generationID int32,
	members []DescribeGroupMember,
) []byte {
	var buf []byte
	buf = AppendInt16(buf, errCode)
	buf = AppendString(buf, groupID)
	buf = AppendString(buf, state)
	buf = AppendInt32(buf, generationID)
	buf = AppendString(buf, leaderID)
	buf = AppendInt32(buf, int32(len(members)))
	for _, m := range members {
		buf = AppendString(buf, m.MemberID)
		buf = AppendString(buf, m.ClientID)
		buf = AppendInt32(buf, int32(len(m.Assignment)))
		for _, a := range m.Assignment {
			buf = AppendString(buf, a.Topic)
			buf = AppendInt32(buf, int32(len(a.Partitions)))
			for _, p := range a.Partitions {
				buf = AppendInt32(buf, p)
			}
		}
	}
	return buf
}
