package storage

import (
	"fmt"
	"testing"
)

func newTestLog(t *testing.T) *Log {
	t.Helper()
	dir := t.TempDir()
	log, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { log.Close() })
	return log
}

func TestLogAppendAndRead(t *testing.T) {
	log := newTestLog(t)

	off, err := log.Append([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != 0 {
		t.Errorf("first offset = %d, want 0", off)
	}

	rec, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	if string(rec.Value) != "value1" {
		t.Errorf("value = %q, want %q", rec.Value, "value1")
	}
	if string(rec.Key) != "key1" {
		t.Errorf("key = %q, want %q", rec.Key, "key1")
	}
}

func TestLogOffsetSequence(t *testing.T) {
	log := newTestLog(t)

	const n = 100
	for i := 0; i < n; i++ {
		off, err := log.Append(nil, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if off != uint64(i) {
			t.Errorf("offset[%d] = %d, want %d", i, off, i)
		}
	}

	for i := 0; i < n; i++ {
		rec, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		want := fmt.Sprintf("msg-%d", i)
		if string(rec.Value) != want {
			t.Errorf("Read(%d): value = %q, want %q", i, rec.Value, want)
		}
	}
}

func TestLogEndOffset(t *testing.T) {
	log := newTestLog(t)

	if leo := log.LogEndOffset(); leo != 0 {
		t.Errorf("empty log LEO = %d, want 0", leo)
	}

	log.Append(nil, []byte("a"))
	log.Append(nil, []byte("b"))

	if leo := log.LogEndOffset(); leo != 2 {
		t.Errorf("LEO after 2 appends = %d, want 2", leo)
	}
}

func TestLogSegmentRolling(t *testing.T) {
	log := newTestLog(t)

	// Write enough data to force at least one segment roll.
	// MaxSegmentBytes = 1 MB; each record ≈ 1040 bytes (1 KB value + headers).
	payload := make([]byte, 1024)
	var appended int
	for log.SegmentCount() < 2 {
		if _, err := log.Append(nil, payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
		appended++
		if appended > 2000 {
			t.Fatal("segment never rolled after 2000 records — check MaxSegmentBytes")
		}
	}

	if log.SegmentCount() < 2 {
		t.Errorf("expected ≥ 2 segments after rolling, got %d", log.SegmentCount())
	}

	// Read a record from the first segment to confirm cross-segment reads work.
	rec, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0) after roll: %v", err)
	}
	if rec.Offset != 0 {
		t.Errorf("rec.Offset = %d, want 0", rec.Offset)
	}

	// Read the most recent record.
	lastOff := log.LogEndOffset() - 1
	rec2, err := log.Read(lastOff)
	if err != nil {
		t.Fatalf("Read(%d): %v", lastOff, err)
	}
	if rec2.Offset != lastOff {
		t.Errorf("rec2.Offset = %d, want %d", rec2.Offset, lastOff)
	}
}

func TestLogReadBatch(t *testing.T) {
	log := newTestLog(t)

	for i := 0; i < 50; i++ {
		log.Append(nil, []byte(fmt.Sprintf("msg-%d", i)))
	}

	records, err := log.ReadBatch(10, 1024*1024)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("ReadBatch returned 0 records")
	}
	if records[0].Offset != 10 {
		t.Errorf("first record offset = %d, want 10", records[0].Offset)
	}
}

func TestLogReopen(t *testing.T) {
	dir := t.TempDir()

	// Write 20 records, close.
	{
		log, err := OpenLog(dir)
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		for i := 0; i < 20; i++ {
			log.Append(nil, []byte(fmt.Sprintf("msg-%d", i)))
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen and verify.
	{
		log, err := OpenLog(dir)
		if err != nil {
			t.Fatalf("reopen OpenLog: %v", err)
		}
		defer log.Close()

		leo := log.LogEndOffset()
		if leo != 20 {
			t.Errorf("recovered LEO = %d, want 20", leo)
		}

		// Verify a specific record is still readable.
		rec, err := log.Read(15)
		if err != nil {
			t.Fatalf("Read(15) after reopen: %v", err)
		}
		if string(rec.Value) != "msg-15" {
			t.Errorf("rec.Value = %q, want %q", rec.Value, "msg-15")
		}

		// Append a new record after recovery.
		off, err := log.Append(nil, []byte("new"))
		if err != nil {
			t.Fatalf("Append after reopen: %v", err)
		}
		if off != 20 {
			t.Errorf("post-recovery offset = %d, want 20", off)
		}
	}
}

func TestLogReadFutureOffset(t *testing.T) {
	log := newTestLog(t)
	log.Append(nil, []byte("only"))

	_, err := log.Read(9999)
	if err == nil {
		t.Error("expected error reading future offset, got nil")
	}
}

func TestLogLargeVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large volume test in short mode")
	}

	log := newTestLog(t)
	const n = 10_000
	payload := []byte("hello, this is a test payload of reasonable size for benchmarking!")

	for i := 0; i < n; i++ {
		if _, err := log.Append(nil, payload); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	// Spot-check random offsets.
	for _, off := range []uint64{0, 1, 100, 1000, 5000, n - 1} {
		rec, err := log.Read(off)
		if err != nil {
			t.Errorf("Read(%d): %v", off, err)
			continue
		}
		if rec.Offset != off {
			t.Errorf("Read(%d): got offset %d", off, rec.Offset)
		}
	}

	if leo := log.LogEndOffset(); leo != n {
		t.Errorf("LEO after %d appends = %d", n, leo)
	}
}
