package consumer_group_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	cg "github.com/Utkarsh272/mini-kafka/internal/consumer_group"
)

// getPartitions stub: topic "events" has 6 partitions, "logs" has 3.
func getPartitions(topic string) (int32, error) {
	switch topic {
	case "events":
		return 6, nil
	case "logs":
		return 3, nil
	case "single":
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown topic %q", topic)
	}
}

func newCoordinator() *cg.Coordinator {
	return cg.NewCoordinator(getPartitions)
}

// joinAsync calls JoinGroup in a goroutine and sends the response on ch.
func joinAsync(coord *cg.Coordinator, groupID, memberID, clientID string, topics []string, ch chan cg.JoinGroupResponse) {
	go func() {
		ch <- coord.JoinGroup(cg.JoinGroupRequest{
			GroupID:        groupID,
			SessionTimeout: 30000,
			MemberID:       memberID,
			ClientID:       clientID,
			Topics:         topics,
		})
	}()
}

// doJoinSync runs a full JoinGroup + SyncGroup cycle for a set of members and
// returns the SyncGroup responses keyed by memberID.
func doJoinSync(t *testing.T, coord *cg.Coordinator, groupID string, memberTopics map[string][]string) map[string]cg.SyncGroupResponse {
	t.Helper()

	type joinResult struct {
		memberID string
		resp     cg.JoinGroupResponse
	}

	results := make(chan joinResult, len(memberTopics))
	for clientID, topics := range memberTopics {
		go func(cid string, tt []string) {
			r := coord.JoinGroup(cg.JoinGroupRequest{
				GroupID:        groupID,
				SessionTimeout: 30000,
				ClientID:       cid,
				Topics:         tt,
			})
			results <- joinResult{memberID: r.MemberID, resp: r}
		}(clientID, topics)
	}

	joinResps := make(map[string]cg.JoinGroupResponse)
	timeout := time.After(3 * time.Second)
	for i := 0; i < len(memberTopics); i++ {
		select {
		case jr := <-results:
			joinResps[jr.memberID] = jr.resp
		case <-timeout:
			t.Fatal("JoinGroup phase timed out")
		}
	}

	syncResults := make(map[string]cg.SyncGroupResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for memberID, jr := range joinResps {
		wg.Add(1)
		go func(mID string, jr cg.JoinGroupResponse) {
			defer wg.Done()
			sr := coord.SyncGroup(cg.SyncGroupRequest{
				GroupID:      groupID,
				GenerationID: jr.GenerationID,
				MemberID:     mID,
				// Empty → coordinator auto-computes via range assignor.
			})
			mu.Lock()
			syncResults[mID] = sr
			mu.Unlock()
		}(memberID, jr)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SyncGroup phase timed out")
	}

	return syncResults
}

// ---- JoinGroup tests -------------------------------------------------------

func TestSingleMemberJoin(t *testing.T) {
	coord := newCoordinator()
	ch := make(chan cg.JoinGroupResponse, 1)
	joinAsync(coord, "g1", "", "client-1", []string{"events"}, ch)

	select {
	case resp := <-ch:
		if resp.ErrorCode != 0 {
			t.Fatalf("JoinGroup error: %d", resp.ErrorCode)
		}
		if resp.MemberID == "" {
			t.Error("MemberID should be assigned")
		}
		if resp.GenerationID != 1 {
			t.Errorf("generation = %d, want 1", resp.GenerationID)
		}
		if resp.LeaderID != resp.MemberID {
			t.Errorf("single member should be leader: leader=%q member=%q",
				resp.LeaderID, resp.MemberID)
		}
		if len(resp.Members) != 1 {
			t.Errorf("leader should see 1 member, got %d", len(resp.Members))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("JoinGroup timed out")
	}
}

func TestTwoMembersJoin(t *testing.T) {
	coord := newCoordinator()
	ch1 := make(chan cg.JoinGroupResponse, 1)
	ch2 := make(chan cg.JoinGroupResponse, 1)

	joinAsync(coord, "g2", "", "client-1", []string{"events"}, ch1)
	joinAsync(coord, "g2", "", "client-2", []string{"events"}, ch2)

	var resps [2]cg.JoinGroupResponse
	timeout := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case r := <-ch1:
			resps[0] = r
		case r := <-ch2:
			resps[1] = r
		case <-timeout:
			t.Fatal("JoinGroup timed out for 2 members")
		}
	}

	for i, r := range resps {
		if r.ErrorCode != 0 {
			t.Errorf("member %d: JoinGroup error %d", i, r.ErrorCode)
		}
		if r.GenerationID != 1 {
			t.Errorf("member %d: generation = %d, want 1", i, r.GenerationID)
		}
	}

	leaderCount := 0
	for _, r := range resps {
		if len(r.Members) > 0 {
			leaderCount++
			if len(r.Members) != 2 {
				t.Errorf("leader should see 2 members, got %d", len(r.Members))
			}
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}

// TestMemberIDPreservedOnRejoin verifies that a member which completes a full
// Join+Sync cycle and then rejoins (e.g. after a rebalance triggered by another
// member leaving) keeps its original member ID.
func TestMemberIDPreservedOnRejoin(t *testing.T) {
	coord := newCoordinator()

	// Full Join+Sync so the group reaches Stable state.
	syncResps := doJoinSync(t, coord, "g3", map[string][]string{
		"client-1": {"events"},
	})
	if len(syncResps) == 0 {
		t.Fatal("doJoinSync returned no responses")
	}

	// Retrieve the assigned member ID from the stable group.
	desc := coord.DescribeGroup("g3")
	if len(desc.Members) == 0 {
		t.Fatal("no members in stable group")
	}
	firstMemberID := desc.Members[0].MemberID

	// Rejoin with the known member ID (simulates a rebalance rejoin).
	ch := make(chan cg.JoinGroupResponse, 1)
	joinAsync(coord, "g3", firstMemberID, "client-1", []string{"events"}, ch)

	select {
	case r := <-ch:
		if r.ErrorCode != 0 {
			t.Fatalf("rejoin JoinGroup error: %d", r.ErrorCode)
		}
		if r.MemberID != firstMemberID {
			t.Errorf("rejoin: memberID changed from %q to %q", firstMemberID, r.MemberID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rejoin timed out")
	}
}

// ---- SyncGroup + range assignment tests ------------------------------------

func TestRangeAssignorSingleMember(t *testing.T) {
	coord := newCoordinator()
	syncResps := doJoinSync(t, coord, "g-range-1", map[string][]string{
		"client-1": {"events"},
	})

	if len(syncResps) != 1 {
		t.Fatalf("expected 1 sync response, got %d", len(syncResps))
	}
	for _, sr := range syncResps {
		if sr.ErrorCode != 0 {
			t.Fatalf("SyncGroup error: %d", sr.ErrorCode)
		}
		if len(sr.Assignment) != 6 {
			t.Errorf("single member should get 6 partitions, got %d", len(sr.Assignment))
		}
	}
}

func TestRangeAssignorTwoMembers(t *testing.T) {
	coord := newCoordinator()
	syncResps := doJoinSync(t, coord, "g-range-2", map[string][]string{
		"client-1": {"events"},
		"client-2": {"events"},
	})

	total := 0
	for _, sr := range syncResps {
		if sr.ErrorCode != 0 {
			t.Errorf("SyncGroup error: %d", sr.ErrorCode)
		}
		total += len(sr.Assignment)
	}
	if total != 6 {
		t.Errorf("total assigned partitions = %d, want 6", total)
	}
	for _, sr := range syncResps {
		if len(sr.Assignment) != 3 {
			t.Errorf("each member should get 3 partitions, got %d", len(sr.Assignment))
		}
	}
}

func TestRangeAssignorThreeMembers(t *testing.T) {
	coord := newCoordinator()
	syncResps := doJoinSync(t, coord, "g-range-3", map[string][]string{
		"c1": {"events"},
		"c2": {"events"},
		"c3": {"events"},
	})

	total := 0
	for _, sr := range syncResps {
		if sr.ErrorCode != 0 {
			t.Errorf("SyncGroup error: %d", sr.ErrorCode)
		}
		total += len(sr.Assignment)
	}
	if total != 6 {
		t.Errorf("total = %d, want 6", total)
	}
}

func TestRangeAssignorMoreMembersThanPartitions(t *testing.T) {
	coord := newCoordinator()
	syncResps := doJoinSync(t, coord, "g-sparse", map[string][]string{
		"c1": {"single"},
		"c2": {"single"},
		"c3": {"single"},
	})

	total := 0
	for _, sr := range syncResps {
		total += len(sr.Assignment)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1 (only 1 partition)", total)
	}
}

// ---- Heartbeat tests -------------------------------------------------------

func TestHeartbeatStable(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-hb", map[string][]string{"c1": {"events"}})

	desc := coord.DescribeGroup("g-hb")
	if len(desc.Members) == 0 {
		t.Fatal("no members in group after sync")
	}
	memberID := desc.Members[0].MemberID

	resp := coord.Heartbeat(cg.HeartbeatRequest{
		GroupID:      "g-hb",
		GenerationID: 1,
		MemberID:     memberID,
	})
	if resp.ErrorCode != 0 {
		t.Errorf("Heartbeat error: %d", resp.ErrorCode)
	}
}

func TestHeartbeatUnknownMember(t *testing.T) {
	coord := newCoordinator()
	resp := coord.Heartbeat(cg.HeartbeatRequest{
		GroupID: "nonexistent", GenerationID: 1, MemberID: "ghost",
	})
	if resp.ErrorCode == 0 {
		t.Error("expected non-zero error for unknown member, got 0")
	}
}

func TestHeartbeatWrongGeneration(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-gen", map[string][]string{"c1": {"events"}})
	desc := coord.DescribeGroup("g-gen")
	memberID := desc.Members[0].MemberID

	resp := coord.Heartbeat(cg.HeartbeatRequest{
		GroupID: "g-gen", GenerationID: 99, MemberID: memberID,
	})
	if resp.ErrorCode == 0 {
		t.Error("expected non-zero error for wrong generation, got 0")
	}
}

// ---- LeaveGroup tests ------------------------------------------------------

func TestLeaveGroup(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-leave", map[string][]string{
		"c1": {"events"},
		"c2": {"events"},
	})

	desc := coord.DescribeGroup("g-leave")
	if len(desc.Members) != 2 {
		t.Fatalf("expected 2 members before leave, got %d", len(desc.Members))
	}
	leavingID := desc.Members[0].MemberID

	resp := coord.LeaveGroup(cg.LeaveGroupRequest{
		GroupID: "g-leave", MemberID: leavingID,
	})
	if resp.ErrorCode != 0 {
		t.Errorf("LeaveGroup error: %d", resp.ErrorCode)
	}

	desc2 := coord.DescribeGroup("g-leave")
	if len(desc2.Members) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(desc2.Members))
	}
}

func TestLeaveGroupAllMembers(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-empty", map[string][]string{"c1": {"logs"}})
	desc := coord.DescribeGroup("g-empty")
	memberID := desc.Members[0].MemberID

	coord.LeaveGroup(cg.LeaveGroupRequest{GroupID: "g-empty", MemberID: memberID})

	desc2 := coord.DescribeGroup("g-empty")
	if desc2.State != "Empty" {
		t.Errorf("state = %q, want Empty", desc2.State)
	}
}

// ---- DescribeGroup tests ---------------------------------------------------

func TestDescribeGroupUnknown(t *testing.T) {
	coord := newCoordinator()
	desc := coord.DescribeGroup("does-not-exist")
	if desc.ErrorCode == 0 {
		t.Error("expected non-zero error for unknown group, got 0")
	}
}

func TestDescribeGroupStable(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-describe", map[string][]string{
		"c1": {"events"},
		"c2": {"events"},
	})

	desc := coord.DescribeGroup("g-describe")
	if desc.State != "Stable" {
		t.Errorf("state = %q, want Stable", desc.State)
	}
	if len(desc.Members) != 2 {
		t.Errorf("members = %d, want 2", len(desc.Members))
	}
	if desc.GenerationID != 1 {
		t.Errorf("generation = %d, want 1", desc.GenerationID)
	}
}

// ---- Rebalance after leave -------------------------------------------------

func TestRebalanceAfterLeave(t *testing.T) {
	coord := newCoordinator()
	doJoinSync(t, coord, "g-rebal", map[string][]string{
		"c1": {"events"},
		"c2": {"events"},
	})

	desc := coord.DescribeGroup("g-rebal")
	leavingID := desc.Members[0].MemberID
	coord.LeaveGroup(cg.LeaveGroupRequest{GroupID: "g-rebal", MemberID: leavingID})

	desc2 := coord.DescribeGroup("g-rebal")
	if desc2.State != "PreRebalance" {
		t.Errorf("state after leave = %q, want PreRebalance", desc2.State)
	}
}
