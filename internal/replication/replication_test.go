package replication_test

import (
	"testing"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/replication"
)

// ---- ISRTracker tests ------------------------------------------------------

func newTracker(leaderID int32, replicas []int32, leo *int64) *replication.ISRTracker {
	return replication.NewISRTracker(leaderID, replicas, func() int64 { return *leo })
}

func TestHighWatermarkNoFollowers(t *testing.T) {
	leo := int64(100)
	tracker := newTracker(1, []int32{1}, &leo)

	if hwm := tracker.HighWatermark(); hwm != 100 {
		t.Errorf("HWM = %d, want 100", hwm)
	}
}

func TestHighWatermarkFollowerCaughtUp(t *testing.T) {
	leo := int64(50)
	tracker := newTracker(1, []int32{1, 2}, &leo)

	tracker.RecordFetch(2, 50)

	if hwm := tracker.HighWatermark(); hwm != 50 {
		t.Errorf("HWM = %d, want 50", hwm)
	}
}

func TestHighWatermarkFollowerBehind(t *testing.T) {
	leo := int64(100)
	tracker := newTracker(1, []int32{1, 2}, &leo)

	tracker.RecordFetch(2, 40)

	// HWM = min(LEO=100, follower=40) = 40
	if hwm := tracker.HighWatermark(); hwm != 40 {
		t.Errorf("HWM = %d, want 40", hwm)
	}
}

func TestHighWatermarkTwoFollowers(t *testing.T) {
	leo := int64(200)
	tracker := newTracker(1, []int32{1, 2, 3}, &leo)

	tracker.RecordFetch(2, 150)
	tracker.RecordFetch(3, 180)

	// min(200, 150, 180) = 150
	if hwm := tracker.HighWatermark(); hwm != 150 {
		t.Errorf("HWM = %d, want 150", hwm)
	}
}

func TestHighWatermarkAdvancesAsFollowerCatchesUp(t *testing.T) {
	leo := int64(100)
	tracker := newTracker(1, []int32{1, 2}, &leo)

	tracker.RecordFetch(2, 30)
	if hwm := tracker.HighWatermark(); hwm != 30 {
		t.Errorf("after first fetch HWM = %d, want 30", hwm)
	}

	tracker.RecordFetch(2, 70)
	if hwm := tracker.HighWatermark(); hwm != 70 {
		t.Errorf("after second fetch HWM = %d, want 70", hwm)
	}

	tracker.RecordFetch(2, 100)
	if hwm := tracker.HighWatermark(); hwm != 100 {
		t.Errorf("after catch-up HWM = %d, want 100", hwm)
	}
}

func TestISRShrinkOnRecordLag(t *testing.T) {
	leo := int64(20_000)
	tracker := replication.NewISRTrackerWithConfig(
		1, []int32{1, 2},
		func() int64 { return leo },
		30*time.Second,
		1_000,
	)

	tracker.RecordFetch(2, 0)
	tracker.ShrinkISR()

	for _, m := range tracker.ISRMembers() {
		if m == 2 {
			t.Error("follower 2 should have been shrunk from ISR due to record lag")
		}
	}
}

func TestISRRejoinAfterCatchUp(t *testing.T) {
	leo := int64(20_000)
	tracker := replication.NewISRTrackerWithConfig(
		1, []int32{1, 2},
		func() int64 { return leo },
		30*time.Second,
		1_000,
	)

	// Force shrink.
	tracker.RecordFetch(2, 0)
	tracker.ShrinkISR()

	// Catch up: lag = 20000 - 19500 = 500 < 1000.
	tracker.RecordFetch(2, 19_500)

	found := false
	for _, m := range tracker.ISRMembers() {
		if m == 2 {
			found = true
		}
	}
	if !found {
		t.Error("follower 2 should have rejoined ISR after catching up")
	}
}

func TestISRShrinkOnTimeLag(t *testing.T) {
	leo := int64(10)
	tracker := replication.NewISRTrackerWithConfig(
		1, []int32{1, 2},
		func() int64 { return leo },
		50*time.Millisecond,
		100_000,
	)

	tracker.RecordFetch(2, 5)
	time.Sleep(100 * time.Millisecond)
	tracker.ShrinkISR()

	for _, m := range tracker.ISRMembers() {
		if m == 2 {
			t.Error("follower 2 should have been shrunk from ISR due to time lag")
		}
	}

	leaderFound := false
	for _, m := range tracker.ISRMembers() {
		if m == 1 {
			leaderFound = true
		}
	}
	if !leaderFound {
		t.Error("leader should always remain in ISR")
	}
}

func TestLeaderAlwaysInISR(t *testing.T) {
	leo := int64(0)
	tracker := newTracker(1, []int32{1}, &leo)

	members := tracker.ISRMembers()
	if len(members) != 1 || members[0] != 1 {
		t.Errorf("ISR members = %v, want [1]", members)
	}
}

func TestReplicaStatesSnapshot(t *testing.T) {
	leo := int64(50)
	tracker := newTracker(1, []int32{1, 2, 3}, &leo)
	tracker.RecordFetch(2, 30)
	tracker.RecordFetch(3, 45)

	states := tracker.ReplicaStates()
	if len(states) != 3 {
		t.Errorf("expected 3 replica states, got %d", len(states))
	}
}

// ---- Wire encode/decode tests ----------------------------------------------

func TestEncodeFetchFollowerRequestRoundtrip(t *testing.T) {
	req := replication.FetchFollowerRequest{
		Topic:      "orders",
		Partition:  0,
		FromOffset: 9999,
		MaxBytes:   512 * 1024,
	}

	frame := replication.EncodeFetchFollowerRequest(1, req)
	// Payload starts after: 4B totalLen + 1B apiKey + 4B corrID + 2B clientIDLen = 11B
	if len(frame) < 11 {
		t.Fatalf("frame too short: %d", len(frame))
	}

	decoded, err := replication.DecodeFetchFollowerRequest(frame[11:])
	if err != nil {
		t.Fatalf("DecodeFetchFollowerRequest: %v", err)
	}

	if decoded.Topic != req.Topic {
		t.Errorf("topic = %q, want %q", decoded.Topic, req.Topic)
	}
	if decoded.Partition != req.Partition {
		t.Errorf("partition = %d, want %d", decoded.Partition, req.Partition)
	}
	if decoded.FromOffset != req.FromOffset {
		t.Errorf("fromOffset = %d, want %d", decoded.FromOffset, req.FromOffset)
	}
	if decoded.MaxBytes != req.MaxBytes {
		t.Errorf("maxBytes = %d, want %d", decoded.MaxBytes, req.MaxBytes)
	}
}

func TestEncodeFetchFollowerResponseRoundtrip(t *testing.T) {
	resp := replication.FetchFollowerResponse{
		ErrorCode: 0,
		LeaderLEO: 500,
		Records: []replication.FetchFollowerRecord{
			{Offset: 100, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 101, Timestamp: 1001, Key: nil, Value: []byte("v2")},
		},
	}

	frame := replication.EncodeFetchFollowerResponse(7, resp)
	// Outer frame: 4B len + 4B corrID + 2B outerErrCode = 10B header
	if len(frame) < 10 {
		t.Fatalf("frame too short: %d bytes", len(frame))
	}

	decoded, err := replication.DecodeFetchFollowerResponse(frame[10:])
	if err != nil {
		t.Fatalf("DecodeFetchFollowerResponse: %v", err)
	}

	if decoded.ErrorCode != 0 {
		t.Errorf("error_code = %d, want 0", decoded.ErrorCode)
	}
	if decoded.LeaderLEO != 500 {
		t.Errorf("leader_leo = %d, want 500", decoded.LeaderLEO)
	}
	if len(decoded.Records) != 2 {
		t.Fatalf("record count = %d, want 2", len(decoded.Records))
	}
	if string(decoded.Records[0].Value) != "v1" {
		t.Errorf("record[0].Value = %q, want %q", decoded.Records[0].Value, "v1")
	}
	if decoded.Records[1].Key != nil {
		t.Errorf("record[1].Key should be nil, got %v", decoded.Records[1].Key)
	}
	if string(decoded.Records[1].Value) != "v2" {
		t.Errorf("record[1].Value = %q, want %q", decoded.Records[1].Value, "v2")
	}
}

func TestEmptyFetchFollowerResponse(t *testing.T) {
	resp := replication.FetchFollowerResponse{
		ErrorCode: 0,
		LeaderLEO: 42,
		Records:   nil,
	}

	frame := replication.EncodeFetchFollowerResponse(1, resp)
	decoded, err := replication.DecodeFetchFollowerResponse(frame[10:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.LeaderLEO != 42 {
		t.Errorf("leader_leo = %d, want 42", decoded.LeaderLEO)
	}
	if len(decoded.Records) != 0 {
		t.Errorf("records = %d, want 0", len(decoded.Records))
	}
}
