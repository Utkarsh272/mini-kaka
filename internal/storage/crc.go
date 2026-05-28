package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// crc32Table uses the IEEE polynomial, same as Kafka.
var crc32Table = crc32.MakeTable(crc32.IEEE)

// checksumPayload computes CRC32 over the non-CRC fields of a record:
// [offset][timestamp][key_len][key][value_len][value]
func checksumPayload(r Record) uint32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, r.Offset)
	_ = binary.Write(buf, binary.BigEndian, r.Timestamp)
	_ = binary.Write(buf, binary.BigEndian, r.KeyLen)
	buf.Write(r.Key)
	_ = binary.Write(buf, binary.BigEndian, r.ValueLen)
	buf.Write(r.Value)
	return crc32.Checksum(buf.Bytes(), crc32Table)
}

// ComputeChecksum returns the CRC32 checksum for a record. Use this before
// encoding to set Record.CRC32.
func ComputeChecksum(r Record) uint32 {
	return checksumPayload(r)
}

// ValidateChecksum returns true if the record's CRC32 field matches the
// computed checksum over its payload. Always call this after decoding a record
// read from disk.
func ValidateChecksum(r Record) bool {
	return r.CRC32 == checksumPayload(r)
}
