package consumer_group

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// GroupState is the consumer group state machine state.
type GroupState int

const (
	GroupEmpty        GroupState = iota
	GroupPreRebalance            // waiting for all members to JoinGroup
	GroupAwaitingSync            // waiting for leader to submit assignment
	GroupStable                  // assignment distributed, consumers fetching
	GroupDead                    // coordinator cleaned up this group
)

func (s GroupState) String() string {
	switch s {
	case GroupEmpty:
		return "Empty"
	case GroupPreRebalance:
		return "PreRebalance"
	case GroupAwaitingSync:
		return "AwaitingSync"
	case GroupStable:
		return "Stable"
	case GroupDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// Member represents one consumer in the group.
type Member struct {
	MemberID      string
	ClientID      string
	Subscription  []string         // topics this member subscribes to
	Assignment    []TopicPartition // assigned after SyncGroup
	LastHeartbeat time.Time
}

// TopicPartition identifies a single partition within a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// JoinGroupRequest carries the fields from the JoinGroup API call.
type JoinGroupRequest struct {
	GroupID        string
	SessionTimeout int32  // ms
	MemberID       string // empty = new member
	ClientID       string // used to generate MemberID for new members
	Topics         []string
}

// JoinGroupResponse is returned to each member after all members have joined.
type JoinGroupResponse struct {
	ErrorCode     int16
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       []MemberMetadata // only populated for the group leader
}

// MemberMetadata is the per-member info sent to the group leader.
type MemberMetadata struct {
	MemberID string
	Topics   []string
}

// SyncGroupRequest carries the assignment computed by the leader.
type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []MemberAssignment // only leader sends non-empty list
}

// MemberAssignment is one member's partition assignment.
type MemberAssignment struct {
	MemberID   string
	Partitions []TopicPartition
}

// SyncGroupResponse carries this member's assignment.
type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []TopicPartition
}

// HeartbeatRequest carries a keepalive from a consumer.
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// HeartbeatResponse is returned to the consumer.
type HeartbeatResponse struct {
	ErrorCode int16
}

// LeaveGroupRequest is sent when a consumer shuts down cleanly.
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// LeaveGroupResponse is returned to the leaving consumer.
type LeaveGroupResponse struct {
	ErrorCode int16
}

// pendingJoin holds a member's join request while waiting for the rebalance
// delay window to close.
type pendingJoin struct {
	req    JoinGroupRequest
	respCh chan JoinGroupResponse
}

// pendingSync holds a member waiting for the leader's SyncGroup call.
type pendingSync struct {
	memberID string
	respCh   chan SyncGroupResponse
}

// group holds the full state of one consumer group.
type group struct {
	mu sync.Mutex

	id           string
	state        GroupState
	generationID int32
	leaderID     string

	members      map[string]*Member
	pendingJoins []pendingJoin
	pendingSyncs []pendingSync
	assignments  map[string][]TopicPartition // memberID → assigned partitions

	sessionTimeout time.Duration
	rebalanceDelay time.Duration
}

func newGroup(id string, sessionTimeout time.Duration) *group {
	return &group{
		id:             id,
		state:          GroupEmpty,
		members:        make(map[string]*Member),
		assignments:    make(map[string][]TopicPartition),
		sessionTimeout: sessionTimeout,
		rebalanceDelay: 500 * time.Millisecond,
	}
}

// Coordinator manages all consumer groups on this broker.
// All methods are safe for concurrent use.
type Coordinator struct {
	mu            sync.Mutex
	groups        map[string]*group
	getPartitions func(topic string) (int32, error)
}

// NewCoordinator creates a Coordinator. getPartitions is called to resolve
// topic → partition count during range assignment.
func NewCoordinator(getPartitions func(topic string) (int32, error)) *Coordinator {
	c := &Coordinator{
		groups:        make(map[string]*group),
		getPartitions: getPartitions,
	}
	go c.heartbeatReaper()
	return c
}

func (c *Coordinator) getOrCreate(groupID string, sessionTimeout time.Duration) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		g = newGroup(groupID, sessionTimeout)
		c.groups[groupID] = g
	}
	return g
}

func (c *Coordinator) get(groupID string) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups[groupID]
}

// ---- JoinGroup -------------------------------------------------------------

// JoinGroup processes a JoinGroup request. Blocks until the rebalance delay
// window closes and returns the result for this member.
func (c *Coordinator) JoinGroup(req JoinGroupRequest) JoinGroupResponse {
	sessionTimeout := time.Duration(req.SessionTimeout) * time.Millisecond
	if sessionTimeout <= 0 {
		sessionTimeout = 30 * time.Second
	}

	g := c.getOrCreate(req.GroupID, sessionTimeout)
	g.mu.Lock()

	// Assign a member ID for new members.
	memberID := req.MemberID
	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", req.ClientID, time.Now().UnixNano())
	}

	// Transition to PreRebalance if currently Stable or Empty.
	if g.state == GroupStable || g.state == GroupEmpty {
		g.state = GroupPreRebalance
		g.generationID++
		g.assignments = make(map[string][]TopicPartition)
	}

	if g.state != GroupPreRebalance {
		g.mu.Unlock()
		return JoinGroupResponse{ErrorCode: errRebalanceInProgress}
	}

	g.members[memberID] = &Member{
		MemberID:      memberID,
		ClientID:      req.ClientID,
		Subscription:  req.Topics,
		LastHeartbeat: time.Now(),
	}

	respCh := make(chan JoinGroupResponse, 1)
	g.pendingJoins = append(g.pendingJoins, pendingJoin{
		req: JoinGroupRequest{
			GroupID:  req.GroupID,
			MemberID: memberID,
			ClientID: req.ClientID,
			Topics:   req.Topics,
		},
		respCh: respCh,
	})

	isFirst := len(g.pendingJoins) == 1
	delay := g.rebalanceDelay
	generationID := g.generationID
	g.mu.Unlock()

	if isFirst {
		go func() {
			time.Sleep(delay)
			c.closeJoinPhase(req.GroupID, generationID)
		}()
	}

	return <-respCh
}

// closeJoinPhase moves the group PreRebalance → AwaitingSync and fans out
// JoinGroup responses to all pending members.
func (c *Coordinator) closeJoinPhase(groupID string, expectedGeneration int32) {
	g := c.get(groupID)
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.state != GroupPreRebalance || g.generationID != expectedGeneration {
		return
	}

	g.state = GroupAwaitingSync

	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	if len(memberIDs) == 0 {
		g.state = GroupEmpty
		return
	}
	g.leaderID = memberIDs[0]

	var allMembers []MemberMetadata
	for _, id := range memberIDs {
		m := g.members[id]
		allMembers = append(allMembers, MemberMetadata{
			MemberID: id,
			Topics:   m.Subscription,
		})
	}

	for _, pj := range g.pendingJoins {
		resp := JoinGroupResponse{
			ErrorCode:     0,
			GenerationID:  g.generationID,
			GroupProtocol: "range",
			LeaderID:      g.leaderID,
			MemberID:      pj.req.MemberID,
		}
		if pj.req.MemberID == g.leaderID {
			resp.Members = allMembers
		}
		pj.respCh <- resp
	}
	g.pendingJoins = g.pendingJoins[:0]
}

// ---- SyncGroup -------------------------------------------------------------

// SyncGroup processes a SyncGroup request. If this member is the leader it
// triggers assignment distribution to all waiting members.
func (c *Coordinator) SyncGroup(req SyncGroupRequest) SyncGroupResponse {
	g := c.get(req.GroupID)
	if g == nil {
		return SyncGroupResponse{ErrorCode: errUnknownMemberID}
	}

	g.mu.Lock()

	if g.state != GroupAwaitingSync {
		g.mu.Unlock()
		return SyncGroupResponse{ErrorCode: errRebalanceInProgress}
	}
	if _, ok := g.members[req.MemberID]; !ok {
		g.mu.Unlock()
		return SyncGroupResponse{ErrorCode: errUnknownMemberID}
	}
	if g.generationID != req.GenerationID {
		g.mu.Unlock()
		return SyncGroupResponse{ErrorCode: errIllegalGeneration}
	}

	if req.MemberID == g.leaderID && len(req.Assignments) > 0 {
		for _, a := range req.Assignments {
			g.assignments[a.MemberID] = a.Partitions
		}
	} else if req.MemberID == g.leaderID {
		// Leader sent empty — auto-compute via range assignor.
		if err := c.computeAssignment(g); err != nil {
			g.mu.Unlock()
			return SyncGroupResponse{ErrorCode: errUnknownError}
		}
	}

	respCh := make(chan SyncGroupResponse, 1)
	g.pendingSyncs = append(g.pendingSyncs, pendingSync{
		memberID: req.MemberID,
		respCh:   respCh,
	})

	allSynced := len(g.pendingSyncs) == len(g.members)
	g.mu.Unlock()

	if allSynced {
		c.flushAssignments(req.GroupID)
	}

	return <-respCh
}

// computeAssignment runs the range assignor. Must be called with g.mu held.
func (c *Coordinator) computeAssignment(g *group) error {
	topicSet := make(map[string]struct{})
	for _, m := range g.members {
		for _, t := range m.Subscription {
			topicSet[t] = struct{}{}
		}
	}

	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	for topic := range topicSet {
		numPartitions, err := c.getPartitions(topic)
		if err != nil || numPartitions == 0 {
			continue
		}

		var subscribers []string
		for _, id := range memberIDs {
			for _, t := range g.members[id].Subscription {
				if t == topic {
					subscribers = append(subscribers, id)
					break
				}
			}
		}
		if len(subscribers) == 0 {
			continue
		}

		partitionsPerMember := (int(numPartitions) + len(subscribers) - 1) / len(subscribers)
		partitionIdx := 0
		for _, memberID := range subscribers {
			end := partitionIdx + partitionsPerMember
			if end > int(numPartitions) {
				end = int(numPartitions)
			}
			for p := partitionIdx; p < end; p++ {
				g.assignments[memberID] = append(g.assignments[memberID], TopicPartition{
					Topic: topic, Partition: int32(p),
				})
			}
			partitionIdx = end
			if partitionIdx >= int(numPartitions) {
				break
			}
		}
	}
	return nil
}

// flushAssignments sends each pending member their assignment and moves to Stable.
func (c *Coordinator) flushAssignments(groupID string) {
	g := c.get(groupID)
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.assignments) == 0 {
		c.computeAssignment(g) //nolint:errcheck
	}

	g.state = GroupStable

	for _, ps := range g.pendingSyncs {
		assignment := g.assignments[ps.memberID]
		if m, ok := g.members[ps.memberID]; ok {
			m.Assignment = assignment
		}
		ps.respCh <- SyncGroupResponse{ErrorCode: 0, Assignment: assignment}
	}
	g.pendingSyncs = g.pendingSyncs[:0]
}

// ---- Heartbeat -------------------------------------------------------------

func (c *Coordinator) Heartbeat(req HeartbeatRequest) HeartbeatResponse {
	g := c.get(req.GroupID)
	if g == nil {
		return HeartbeatResponse{ErrorCode: errUnknownMemberID}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.members[req.MemberID]
	if !ok {
		return HeartbeatResponse{ErrorCode: errUnknownMemberID}
	}
	if g.generationID != req.GenerationID {
		return HeartbeatResponse{ErrorCode: errIllegalGeneration}
	}
	if g.state == GroupPreRebalance || g.state == GroupAwaitingSync {
		return HeartbeatResponse{ErrorCode: errRebalanceInProgress}
	}

	m.LastHeartbeat = time.Now()
	return HeartbeatResponse{ErrorCode: 0}
}

// ---- LeaveGroup ------------------------------------------------------------

func (c *Coordinator) LeaveGroup(req LeaveGroupRequest) LeaveGroupResponse {
	g := c.get(req.GroupID)
	if g == nil {
		return LeaveGroupResponse{ErrorCode: errUnknownMemberID}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.members[req.MemberID]; !ok {
		return LeaveGroupResponse{ErrorCode: errUnknownMemberID}
	}

	delete(g.members, req.MemberID)
	delete(g.assignments, req.MemberID)

	if len(g.members) == 0 {
		g.state = GroupEmpty
		g.leaderID = ""
	} else if g.state == GroupStable {
		g.state = GroupPreRebalance
		g.generationID++
		g.assignments = make(map[string][]TopicPartition)
	}

	return LeaveGroupResponse{ErrorCode: 0}
}

// ---- DescribeGroup ---------------------------------------------------------

type GroupDescription struct {
	ErrorCode    int16
	GroupID      string
	State        string
	GenerationID int32
	LeaderID     string
	Members      []MemberDescription
}

type MemberDescription struct {
	MemberID   string
	ClientID   string
	Assignment []TopicPartition
}

func (c *Coordinator) DescribeGroup(groupID string) GroupDescription {
	g := c.get(groupID)
	if g == nil {
		return GroupDescription{ErrorCode: errUnknownMemberID, GroupID: groupID}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	desc := GroupDescription{
		GroupID:      groupID,
		State:        g.state.String(),
		GenerationID: g.generationID,
		LeaderID:     g.leaderID,
	}
	for _, m := range g.members {
		desc.Members = append(desc.Members, MemberDescription{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			Assignment: m.Assignment,
		})
	}
	sort.Slice(desc.Members, func(i, j int) bool {
		return desc.Members[i].MemberID < desc.Members[j].MemberID
	})
	return desc
}

// ---- Heartbeat reaper ------------------------------------------------------

func (c *Coordinator) heartbeatReaper() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.reapStaleMembers()
	}
}

func (c *Coordinator) reapStaleMembers() {
	c.mu.Lock()
	groupIDs := make([]string, 0, len(c.groups))
	for id := range c.groups {
		groupIDs = append(groupIDs, id)
	}
	c.mu.Unlock()

	for _, gid := range groupIDs {
		g := c.get(gid)
		if g == nil {
			continue
		}
		g.mu.Lock()
		var evicted []string
		for id, m := range g.members {
			if time.Since(m.LastHeartbeat) > g.sessionTimeout {
				evicted = append(evicted, id)
			}
		}
		for _, id := range evicted {
			delete(g.members, id)
			delete(g.assignments, id)
		}
		if len(evicted) > 0 {
			if len(g.members) == 0 {
				g.state = GroupEmpty
				g.leaderID = ""
			} else if g.state == GroupStable {
				g.state = GroupPreRebalance
				g.generationID++
				g.assignments = make(map[string][]TopicPartition)
			}
		}
		g.mu.Unlock()
	}
}

// ---- Error codes (Kafka-compatible subset) ---------------------------------

const (
	errRebalanceInProgress int16 = 27
	errUnknownMemberID     int16 = 25
	errIllegalGeneration   int16 = 22
	errUnknownError        int16 = -1
)
