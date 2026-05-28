package storage

import (
	"bytes"
	"encoding/binary"
)

// Record matches your exact byte specification:
// [length: 4B][offset: 8B][timestamp: 8B][crc32: 4B][key_len: 4B][key bytes][value_len: 4B][value bytes]
type Record struct {
	Length    uint32
	Offset    uint64
	Timestamp uint64
	CRC32     uint32
	KeyLen    uint32
	Key       []byte
	ValueLen  uint32
	Value     []byte
}

// Encode serializes a Record struct into a binary byte slice.
func Encode(r Record) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write fixed-size fields in order (using BigEndian)
	if err := binary.Write(buf, binary.BigEndian, r.Length); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.CRC32); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.KeyLen); err != nil {
		return nil, err
	}

	// Write variable key bytes if they exist
	if len(r.Key) > 0 {
		if _, err := buf.Write(r.Key); err != nil {
			return nil, err
		}
	}

	// Write variable value length and bytes
	if err := binary.Write(buf, binary.BigEndian, r.ValueLen); err != nil {
		return nil, err
	}
	if len(r.Value) > 0 {
		if _, err := buf.Write(r.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Decode deserializes a binary byte slice back into a Record struct.
func Decode(data []byte) (Record, error) {
	r := Record{}
	buf := bytes.NewReader(data)

	// Read fixed-size headers
	if err := binary.Read(buf, binary.BigEndian, &r.Length); err != nil {
		return r, err
	}
	if err := binary.Read(buf, binary.BigEndian, &r.Offset); err != nil {
		return r, err
	}
	if err := binary.Read(buf, binary.BigEndian, &r.Timestamp); err != nil {
		return r, err
	}
	if err := binary.Read(buf, binary.BigEndian, &r.CRC32); err != nil {
		return r, err
	}
	if err := binary.Read(buf, binary.BigEndian, &r.KeyLen); err != nil {
		return r, err
	}

	// Read Key bytes only if KeyLen is greater than 0
	if r.KeyLen > 0 {
		r.Key = make([]byte, r.KeyLen)
		if _, err := buf.Read(r.Key); err != nil {
			return r, err
		}
	}

	// Read ValueLen
	if err := binary.Read(buf, binary.BigEndian, &r.ValueLen); err != nil {
		return r, err
	}

	// Read Value bytes only if ValueLen is greater than 0
	if r.ValueLen > 0 {
		r.Value = make([]byte, r.ValueLen)
		if _, err := buf.Read(r.Value); err != nil {
			return r, err
		}
	}

	return r, nil
}
