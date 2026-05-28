package storage

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	tests := []struct {
		name    string
		input   Record
		wantErr bool
	}{
		{
			name: "Standard Record",
			input: Record{
				Length:    54,
				Offset:    5012,
				Timestamp: 1716760000,
				CRC32:     0xEDB88320,
				KeyLen:    6,
				Key:       []byte("userID"),
				ValueLen:  14,
				Value:     []byte("john_doe_value"),
			},
			wantErr: false,
		},
		{
			name: "Empty Key and Value",
			input: Record{
				Length:    32,
				Offset:    999,
				Timestamp: 1716760000,
				CRC32:     0,
				KeyLen:    0,
				Key:       nil,
				ValueLen:  0,
				Value:     nil,
			},
			wantErr: false,
		},
		{
			name: "Large value",
			input: Record{
				Length:    1048,
				Offset:    100000,
				Timestamp: 1716760001,
				CRC32:     0xDEADBEEF,
				KeyLen:    4,
				Key:       []byte("test"),
				ValueLen:  1024,
				Value:     bytes.Repeat([]byte("x"), 1024),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Encode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			if decoded.Length != tt.input.Length {
				t.Errorf("Length mismatch: got %d, want %d", decoded.Length, tt.input.Length)
			}
			if decoded.Offset != tt.input.Offset {
				t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, tt.input.Offset)
			}
			if decoded.Timestamp != tt.input.Timestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, tt.input.Timestamp)
			}
			if decoded.CRC32 != tt.input.CRC32 {
				t.Errorf("CRC32 mismatch: got %d, want %d", decoded.CRC32, tt.input.CRC32)
			}
			if decoded.KeyLen != tt.input.KeyLen {
				t.Errorf("KeyLen mismatch: got %d, want %d", decoded.KeyLen, tt.input.KeyLen)
			}
			if !bytes.Equal(decoded.Key, tt.input.Key) {
				t.Errorf("Key bytes mismatch: got %q, want %q", decoded.Key, tt.input.Key)
			}
			if decoded.ValueLen != tt.input.ValueLen {
				t.Errorf("ValueLen mismatch: got %d, want %d", decoded.ValueLen, tt.input.ValueLen)
			}
			if !bytes.Equal(decoded.Value, tt.input.Value) {
				t.Errorf("Value bytes mismatch: got %q, want %q", decoded.Value, tt.input.Value)
			}
		})
	}
}

func TestDecodeCorruptBytes(t *testing.T) {
	corruptData := []byte{0x00, 0x01, 0x02}
	_, err := Decode(corruptData)
	if err == nil {
		t.Error("Expected error when decoding truncated bytes, but got nil")
	}
}

func TestEncodedSize(t *testing.T) {
	r := Record{
		Length:    0, // will be calculated
		Offset:    1,
		Timestamp: 1000,
		CRC32:     0xABCD1234,
		KeyLen:    3,
		Key:       []byte("key"),
		ValueLen:  5,
		Value:     []byte("value"),
	}
	encoded, err := Encode(r)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	// Fixed: 4+8+8+4+4+4 = 32 bytes of headers. Variable: 3 key + 5 value = 8 bytes. Total = 40.
	expectedSize := 32 + 3 + 5
	if len(encoded) != expectedSize {
		t.Errorf("Encoded size = %d, want %d", len(encoded), expectedSize)
	}
}
