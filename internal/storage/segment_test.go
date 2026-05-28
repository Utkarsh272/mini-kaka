package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// newTestSegment creates a segment in a temp dir and registers cleanup.
func newTestSegment(t *testing.T) (*Segment, string) {
	t.Helper()
	dir := t.TempDir()
	seg, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	t.Cleanup(func() { seg.close() })
	return seg, dir
}

func TestSegmentAppendAndReadAt(t *testing.T) {
	seg, _ := newTestSegment(t)

	key := []byte("k1")
	value := []byte("hello world")

	offset, err := seg.append(Record{
		Timestamp: 1000,
		KeyLen:    uint32(len(key)),
		Key:       key,
		ValueLen:  uint32(len(value)),
		Value:     value,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset = %d, want 0", offset)
	}

	got, err := seg.readAt(0)
	if err != nil {
		t.Fatalf("readAt(0): %v", err)
	}
	if string(got.Value) != string(value) {
		t.Errorf("value = %q, want %q", got.Value, value)
	}
	if string(got.Key) != string(key) {
		t.Errorf("key = %q, want %q", got.Key, key)
	}
}

func TestSegmentOffsetSequence(t *testing.T) {
	seg, _ := newTestSegment(t)

	for i := 0; i < 10; i++ {
		off, err := seg.append(Record{
			Timestamp: 1000,
			KeyLen:    0,
			ValueLen:  uint32(len("v")),
			Value:     []byte("v"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Errorf("offset[%d] = %d, want %d", i, off, i)
		}
	}

	for i := 0; i < 10; i++ {
		rec, err := seg.readAt(uint64(i))
		if err != nil {
			t.Fatalf("readAt(%d): %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Errorf("rec.Offset = %d, want %d", rec.Offset, i)
		}
	}
}

func TestSegmentChecksumValidation(t *testing.T) {
	seg, dir := newTestSegment(t)

	_, err := seg.append(Record{
		Timestamp: 1000,
		KeyLen:    3,
		Key:       []byte("key"),
		ValueLen:  5,
		Value:     []byte("value"),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Corrupt bytes 20–21 of the log file (the CRC32 field starts at byte 20:
	// length[4] + offset[8] + timestamp[8] = 20).
	// We open a separate file handle so we can use WriteAt without being
	// affected by O_APPEND or the segment's internal seek position.
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open log for corruption: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF, 0xFF}, 20); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}
	f.Close()

	_, err = seg.readAt(0)
	if err == nil {
		t.Error("expected checksum error after corruption, got nil")
	}
}

func TestSegmentOutOfRange(t *testing.T) {
	seg, _ := newTestSegment(t)

	// No records written yet — any read should fail.
	_, err := seg.readAt(0)
	if err == nil {
		t.Error("expected error reading from empty segment, got nil")
	}

	// Write one record and try a far-future offset.
	seg.append(Record{ValueLen: 1, Value: []byte("x")})
	_, err = seg.readAt(99)
	if err == nil {
		t.Error("expected error reading non-existent offset 99")
	}
}

func TestSegmentReopen(t *testing.T) {
	dir := t.TempDir()

	seg, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	for i := 0; i < 5; i++ {
		seg.append(Record{ValueLen: 1, Value: []byte("v")})
	}
	if err := seg.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seg2, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("reopen segment: %v", err)
	}
	defer seg2.close()

	if seg2.nextOffset != 5 {
		t.Errorf("recovered nextOffset = %d, want 5", seg2.nextOffset)
	}

	off, err := seg2.append(Record{ValueLen: 3, Value: []byte("new")})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if off != 5 {
		t.Errorf("post-recovery offset = %d, want 5", off)
	}
}

func TestSegmentRollCondition(t *testing.T) {
	seg, _ := newTestSegment(t)

	if seg.isFull() {
		t.Error("new segment should not be full")
	}

	seg.mu.Lock()
	seg.logSize = MaxSegmentBytes
	seg.mu.Unlock()

	if !seg.isFull() {
		t.Error("segment with logSize >= MaxSegmentBytes should be full")
	}
}

func TestSegmentReadBatch(t *testing.T) {
	seg, _ := newTestSegment(t)

	for i := 0; i < 20; i++ {
		seg.append(Record{
			Timestamp: uint64(i),
			ValueLen:  5,
			Value:     []byte("hello"),
		})
	}

	records, err := seg.readFrom(5, 1024*1024)
	if err != nil {
		t.Fatalf("readFrom: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected records, got 0")
	}
	if records[0].Offset != 5 {
		t.Errorf("first record offset = %d, want 5", records[0].Offset)
	}
}

func TestIndexFilesCreated(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer seg.close()

	seg.append(Record{ValueLen: 3, Value: []byte("abc")})

	if _, err := os.Stat(dir + "/00000000000000000000.log"); err != nil {
		t.Error("missing .log file")
	}
	if _, err := os.Stat(dir + "/00000000000000000000.index"); err != nil {
		t.Error("missing .index file")
	}
}
