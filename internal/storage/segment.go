package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// indexEntrySize is the size of a single index entry: [relativeOffset: 4B][bytePosition: 4B]
	indexEntrySize = 8

	// indexIntervalBytes controls sparseness: one index entry per this many bytes written.
	// Kafka default is 4096; we use 512 to make it observable in tests.
	indexIntervalBytes = 512

	// MaxSegmentBytes is the log file size at which the segment is rolled.
	// We use 1 MB; Kafka defaults to 1 GB.
	MaxSegmentBytes = 1 * 1024 * 1024 // 1 MB
)

// indexEntry is an in-memory representation of a single .index file entry.
type indexEntry struct {
	relativeOffset uint32 // offset relative to segment baseOffset
	bytePosition   uint32 // byte position in the .log file
}

// Segment represents a single pair of .log + .index files.
// A partition log is a sequence of segments.
//
// File naming: both files share the same base name, which is the 20-digit
// zero-padded base offset of the first record in the segment:
//
//	00000000000000000000.log
//	00000000000000000000.index
type Segment struct {
	mu sync.Mutex

	baseOffset uint64 // offset of the first record in this segment
	nextOffset uint64 // next offset to be assigned (= last written offset + 1)

	logFile   *os.File
	indexFile *os.File

	logSize           int64 // current byte size of the log file (= next write position)
	bytesSinceLastIdx int64 // bytes written since the last index entry
}

// openSegment opens (or creates) a segment with the given base offset in dir.
// Log file is opened with O_RDWR (no O_APPEND) so we can seek for both reads
// and targeted corruption tests. We track the write position in s.logSize and
// use WriteAt to append, which is safe on local filesystems.
// Index file uses O_APPEND since entries are always written sequentially.
func openSegment(dir string, baseOffset uint64) (*Segment, error) {
	baseName := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, baseName+".log")
	idxPath := filepath.Join(dir, baseName+".index")

	// O_RDWR only — no O_APPEND so Seek+WriteAt work correctly.
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	// Index is append-only; O_APPEND is fine here.
	indexFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file: %w", err)
	}

	logInfo, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}
	logSize := logInfo.Size()

	nextOffset := baseOffset
	if logSize > 0 {
		last, err := lastOffsetInLog(logFile)
		if err != nil {
			logFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("recover nextOffset: %w", err)
		}
		nextOffset = last + 1
	}

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: nextOffset,
		logFile:    logFile,
		indexFile:  indexFile,
		logSize:    logSize,
	}, nil
}

// lastOffsetInLog scans the log file from the beginning and returns the offset
// of the last valid record. Used during recovery only.
func lastOffsetInLog(f *os.File) (uint64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var lastOffset uint64
	for {
		var length uint32
		if err := binary.Read(f, binary.BigEndian, &length); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		var offset uint64
		if err := binary.Read(f, binary.BigEndian, &offset); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		lastOffset = offset
		// Skip the rest: length encodes bytes after the length field;
		// we already consumed 8 bytes (offset), so skip length-8 more.
		remaining := int64(length) - 8
		if remaining < 0 {
			break
		}
		if _, err := f.Seek(remaining, io.SeekCurrent); err != nil {
			break
		}
	}
	return lastOffset, nil
}

// append writes a new record to the segment and returns the assigned offset.
// The caller sets Key/KeyLen/Value/ValueLen; Offset, CRC32, Length, Timestamp
// are all assigned here.
func (s *Segment) append(r Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r.Offset = s.nextOffset
	r.CRC32 = ComputeChecksum(r)
	// Length = bytes after the 4-byte length field itself.
	r.Length = 8 + 8 + 4 + 4 + r.KeyLen + 4 + r.ValueLen

	data, err := Encode(r)
	if err != nil {
		return 0, fmt.Errorf("encode record: %w", err)
	}

	bytePos := s.logSize

	// Write a sparse index entry at the start of the segment or every
	// indexIntervalBytes bytes.
	if s.bytesSinceLastIdx >= indexIntervalBytes || s.logSize == 0 {
		relOff := uint32(s.nextOffset - s.baseOffset)
		if err := writeIndexEntry(s.indexFile, indexEntry{
			relativeOffset: relOff,
			bytePosition:   uint32(bytePos),
		}); err != nil {
			return 0, fmt.Errorf("write index entry: %w", err)
		}
		s.bytesSinceLastIdx = 0
	}

	// WriteAt writes at an explicit position and is not affected by the file's
	// seek pointer. Safe to call concurrently with reads that use Seek+Read
	// because we hold the mutex.
	n, err := s.logFile.WriteAt(data, bytePos)
	if err != nil {
		return 0, fmt.Errorf("write log: %w", err)
	}

	written := int64(n)
	s.logSize += written
	s.bytesSinceLastIdx += written
	s.nextOffset++

	return r.Offset, nil
}

// readAt returns the record at the given absolute offset.
func (s *Segment) readAt(offset uint64) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return Record{}, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	bytePos, err := s.indexLookup(offset)
	if err != nil {
		return Record{}, err
	}

	if _, err := s.logFile.Seek(bytePos, io.SeekStart); err != nil {
		return Record{}, fmt.Errorf("seek log: %w", err)
	}

	for {
		rec, _, err := readOneRecord(s.logFile)
		if err != nil {
			return Record{}, err
		}
		if rec.Offset == offset {
			if !ValidateChecksum(rec) {
				return Record{}, fmt.Errorf("checksum mismatch at offset %d", offset)
			}
			return rec, nil
		}
		if rec.Offset > offset {
			return Record{}, fmt.Errorf("offset %d not found in segment", offset)
		}
	}
}

// readFrom returns up to maxBytes of records starting at offset (inclusive).
func (s *Segment) readFrom(offset uint64, maxBytes int64) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	bytePos, err := s.indexLookup(offset)
	if err != nil {
		return nil, err
	}

	if _, err := s.logFile.Seek(bytePos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek log: %w", err)
	}

	var records []Record
	var bytesRead int64

	for bytesRead < maxBytes {
		rec, n, err := readOneRecord(s.logFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		if rec.Offset < offset {
			bytesRead += int64(n)
			continue
		}

		if !ValidateChecksum(rec) {
			return nil, fmt.Errorf("checksum mismatch at offset %d", rec.Offset)
		}
		records = append(records, rec)
		bytesRead += int64(n)
	}

	return records, nil
}

// indexLookup returns the byte position in the log file for the largest index
// entry whose relativeOffset ≤ (offset - baseOffset).
func (s *Segment) indexLookup(offset uint64) (int64, error) {
	idxInfo, err := s.indexFile.Stat()
	if err != nil {
		return 0, err
	}
	numEntries := idxInfo.Size() / indexEntrySize
	if numEntries == 0 {
		return 0, nil
	}

	entries := make([]indexEntry, numEntries)
	if _, err := s.indexFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	for i := int64(0); i < numEntries; i++ {
		var e indexEntry
		if err := binary.Read(s.indexFile, binary.BigEndian, &e.relativeOffset); err != nil {
			return 0, err
		}
		if err := binary.Read(s.indexFile, binary.BigEndian, &e.bytePosition); err != nil {
			return 0, err
		}
		entries[i] = e
	}

	target := uint32(offset - s.baseOffset)
	pos := sort.Search(len(entries), func(i int) bool {
		return entries[i].relativeOffset > target
	}) - 1

	if pos < 0 {
		return 0, nil
	}
	return int64(entries[pos].bytePosition), nil
}

// isFull reports whether this segment has exceeded MaxSegmentBytes.
func (s *Segment) isFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logSize >= MaxSegmentBytes
}

// close flushes and closes the segment's files.
func (s *Segment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.indexFile.Sync(); err != nil {
		return err
	}
	s.logFile.Close()
	s.indexFile.Close()
	return nil
}

// writeIndexEntry appends a single index entry to the index file.
func writeIndexEntry(f *os.File, e indexEntry) error {
	buf := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint32(buf[0:4], e.relativeOffset)
	binary.BigEndian.PutUint32(buf[4:8], e.bytePosition)
	_, err := f.Write(buf)
	return err
}

// readOneRecord reads the next record from r and returns it along with the
// number of bytes consumed.
func readOneRecord(r io.Reader) (Record, int, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return Record{}, 0, err
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, 4, io.ErrUnexpectedEOF
	}

	full := make([]byte, 4+int(length))
	binary.BigEndian.PutUint32(full[:4], length)
	copy(full[4:], payload)

	rec, err := Decode(full)
	if err != nil {
		return Record{}, 4 + int(length), err
	}
	return rec, 4 + int(length), nil
}
