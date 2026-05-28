// Package protocol implements the mini-kafka wire protocol.
//
// Every request is framed as:
//
//	[length: 4B][api_key: 1B][correlation_id: 4B][client_id_len: 2B][client_id: N bytes][payload]
//
// Every response is framed as:
//
//	[length: 4B][correlation_id: 4B][error_code: 2B][payload]
//
// All integers are big-endian. length does not include the 4-byte length field itself.
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RequestHeader is parsed from every incoming TCP frame before the payload.
type RequestHeader struct {
	APIKey        APIKey
	CorrelationID uint32
	ClientID      string
}

// ResponseHeader is written at the start of every outgoing TCP frame.
type ResponseHeader struct {
	CorrelationID uint32
	ErrorCode     ErrorCode
}

// ReadRequest reads one complete request from r.
// Returns the header and the raw payload bytes (everything after the client_id).
// Blocks until a full frame is available or returns an error (including io.EOF).
func ReadRequest(r io.Reader) (RequestHeader, []byte, error) {
	// Read 4-byte total length.
	var totalLen uint32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return RequestHeader{}, nil, err
	}
	if totalLen < 7 { // api_key(1) + corr_id(4) + client_id_len(2) minimum
		return RequestHeader{}, nil, fmt.Errorf("frame too short: %d bytes", totalLen)
	}

	// Read the entire frame body into a buffer so we can parse it safely.
	body := make([]byte, totalLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return RequestHeader{}, nil, fmt.Errorf("read frame body: %w", err)
	}

	pos := 0

	// api_key: 1 byte
	apiKey := APIKey(body[pos])
	pos++

	// correlation_id: 4 bytes
	corrID := binary.BigEndian.Uint32(body[pos:])
	pos += 4

	// client_id: 2-byte length-prefixed string
	clientIDLen := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	if pos+clientIDLen > len(body) {
		return RequestHeader{}, nil, fmt.Errorf("client_id overflows frame")
	}
	clientID := string(body[pos : pos+clientIDLen])
	pos += clientIDLen

	hdr := RequestHeader{
		APIKey:        apiKey,
		CorrelationID: corrID,
		ClientID:      clientID,
	}
	return hdr, body[pos:], nil
}

// WriteResponse writes a complete response frame to w.
// payload is the already-serialized response body (after the fixed header).
func WriteResponse(w io.Writer, corrID uint32, errCode ErrorCode, payload []byte) error {
	// length = corr_id(4) + error_code(2) + len(payload)
	totalLen := uint32(4 + 2 + len(payload))

	buf := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(buf[0:], totalLen)
	binary.BigEndian.PutUint32(buf[4:], corrID)
	binary.BigEndian.PutUint16(buf[8:], uint16(errCode))
	copy(buf[10:], payload)

	_, err := w.Write(buf)
	return err
}

// EncodeString encodes a length-prefixed string (2-byte length + bytes).
func EncodeString(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b[:2], uint16(len(s)))
	copy(b[2:], s)
	return b
}

// DecodeString reads a 2-byte length-prefixed string from data at offset pos.
// Returns the string and the new position.
func DecodeString(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", pos, fmt.Errorf("not enough data for string length at pos %d", pos)
	}
	l := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+l > len(data) {
		return "", pos, fmt.Errorf("string data overflows buffer at pos %d", pos)
	}
	return string(data[pos : pos+l]), pos + l, nil
}

// DecodeInt32 reads a big-endian int32 from data at pos.
func DecodeInt32(data []byte, pos int) (int32, int, error) {
	if pos+4 > len(data) {
		return 0, pos, fmt.Errorf("not enough data for int32 at pos %d", pos)
	}
	v := int32(binary.BigEndian.Uint32(data[pos:]))
	return v, pos + 4, nil
}

// DecodeInt64 reads a big-endian int64 from data at pos.
func DecodeInt64(data []byte, pos int) (int64, int, error) {
	if pos+8 > len(data) {
		return 0, pos, fmt.Errorf("not enough data for int64 at pos %d", pos)
	}
	v := int64(binary.BigEndian.Uint64(data[pos:]))
	return v, pos + 8, nil
}

// AppendUint32 appends a big-endian uint32 to buf and returns the result.
func AppendUint32(buf []byte, v uint32) []byte {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

// AppendInt32 appends a big-endian int32 to buf and returns the result.
func AppendInt32(buf []byte, v int32) []byte {
	return AppendUint32(buf, uint32(v))
}

// AppendInt64 appends a big-endian int64 to buf and returns the result.
func AppendInt64(buf []byte, v int64) []byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return append(buf, b[:]...)
}

// AppendInt16 appends a big-endian int16 to buf and returns the result.
func AppendInt16(buf []byte, v int16) []byte {
	b := [2]byte{}
	binary.BigEndian.PutUint16(b[:], uint16(v))
	return append(buf, b[:]...)
}

// AppendString appends a 2-byte length-prefixed string to buf.
func AppendString(buf []byte, s string) []byte {
	buf = AppendInt16(buf, int16(len(s)))
	return append(buf, s...)
}

// AppendBytes appends a 4-byte length-prefixed byte slice to buf.
// A nil/empty slice is encoded as length -1 (null bytes).
func AppendBytes(buf []byte, b []byte) []byte {
	if b == nil {
		return AppendInt32(buf, -1)
	}
	buf = AppendInt32(buf, int32(len(b)))
	return append(buf, b...)
}
