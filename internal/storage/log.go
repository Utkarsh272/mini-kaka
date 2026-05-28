package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Log is a partition's append-only log. It is a sequence of Segments on disk.
// New records are always appended to the active (last) segment. When the active
// segment exceeds MaxSegmentBytes, a new segment is created.
//
// Directory layout on disk:
//
//	<dir>/
//	├── 00000000000000000000.log
//	├── 00000000000000000000.index
//	├── 00000000000000001024.log     (rolled when the previous .log hit 1 MB)
//	└── 00000000000000001024.index
type Log struct {
	mu       sync.RWMutex
	dir      string
	segments []*Segment
	active   *Segment // always segments[len(segments)-1]
}

// OpenLog opens (or creates) the log in the given directory.
// If the directory contains existing segments they are recovered in order.
func OpenLog(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	// Find all .log files in the directory.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read log dir: %w", err)
	}

	var baseOffsets []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".log")
		off, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	// If no segments exist, create the initial one at offset 0.
	if len(baseOffsets) == 0 {
		baseOffsets = []uint64{0}
	}

	segments := make([]*Segment, 0, len(baseOffsets))
	for _, off := range baseOffsets {
		seg, err := openSegment(dir, off)
		if err != nil {
			// Close already-opened segments before returning.
			for _, s := range segments {
				s.close()
			}
			return nil, fmt.Errorf("open segment %d: %w", off, err)
		}
		segments = append(segments, seg)
	}

	l := &Log{
		dir:      dir,
		segments: segments,
		active:   segments[len(segments)-1],
	}
	return l, nil
}

// Append adds a new record to the log.
//
// The caller provides Key and Value; Offset, CRC32, Timestamp, and Length are
// all set by Append. Returns the assigned offset.
func (l *Log) Append(key, value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Roll segment if the active one is full.
	if l.active.isFull() {
		if err := l.rollSegment(); err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
	}

	r := Record{
		Timestamp: uint64(time.Now().UnixMilli()),
		KeyLen:    uint32(len(key)),
		Key:       key,
		ValueLen:  uint32(len(value)),
		Value:     value,
	}

	return l.active.append(r)
}

// Read returns the record at the given absolute offset.
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seg, err := l.findSegment(offset)
	if err != nil {
		return Record{}, err
	}
	return seg.readAt(offset)
}

// ReadBatch returns up to maxBytes of records starting at startOffset.
// Useful for the consumer Fetch path.
func (l *Log) ReadBatch(startOffset uint64, maxBytes int64) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seg, err := l.findSegment(startOffset)
	if err != nil {
		return nil, err
	}
	return seg.readFrom(startOffset, maxBytes)
}

// LogEndOffset returns the next offset to be written (i.e. last written + 1).
// This mirrors Kafka's concept of the log end offset (LEO).
func (l *Log) LogEndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active.nextOffset
}

// OldestOffset returns the base offset of the oldest segment.
func (l *Log) OldestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

// Close flushes and closes all segment files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if err := seg.close(); err != nil {
			return err
		}
	}
	return nil
}

// --- internal helpers ---

// findSegment returns the segment that contains the given offset using binary
// search over the ordered segment base offsets.
func (l *Log) findSegment(offset uint64) (*Segment, error) {
	if len(l.segments) == 0 {
		return nil, fmt.Errorf("log has no segments")
	}

	// Binary search: find the last segment whose baseOffset ≤ offset.
	pos := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1

	if pos < 0 {
		return nil, fmt.Errorf("offset %d is before the oldest segment (base %d)", offset, l.segments[0].baseOffset)
	}

	seg := l.segments[pos]
	if offset >= seg.nextOffset {
		return nil, fmt.Errorf("offset %d not yet written (log end offset is %d)", offset, l.active.nextOffset)
	}
	return seg, nil
}

// rollSegment closes the current active segment (by stopping writes to it) and
// opens a new segment starting at the current log end offset. Must be called
// with l.mu held for writing.
func (l *Log) rollSegment() error {
	// The new segment's baseOffset = current active's nextOffset.
	newBaseOffset := l.active.nextOffset

	newSeg, err := openSegment(l.dir, newBaseOffset)
	if err != nil {
		return err
	}

	// The old active segment stays open for reads; we just stop appending to it.
	l.segments = append(l.segments, newSeg)
	l.active = newSeg
	return nil
}

// DeleteOldSegments removes all segments whose nextOffset is ≤ retainOffset.
// Useful for log retention (not wired to the broker yet, but complete here).
func (l *Log) DeleteOldSegments(retainOffset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var keep []*Segment
	for _, seg := range l.segments {
		if seg.nextOffset <= retainOffset && seg != l.active {
			seg.close()
			// Delete both files.
			baseName := fmt.Sprintf("%020d", seg.baseOffset)
			os.Remove(filepath.Join(l.dir, baseName+".log"))
			os.Remove(filepath.Join(l.dir, baseName+".index"))
		} else {
			keep = append(keep, seg)
		}
	}
	l.segments = keep
	return nil
}

// SegmentCount returns the number of segments in the log (for observability).
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// Ensure Log implements io.Closer.
var _ io.Closer = (*Log)(nil)
