package server_test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
	"github.com/Utkarsh272/mini-kafka/internal/protocol"
	"github.com/Utkarsh272/mini-kafka/internal/server"
)

type testClient struct {
	conn   net.Conn
	br     *bufio.Reader
	bw     *bufio.Writer
	corrID uint32
}

func newTestClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{
		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
}

func (c *testClient) send(apiKey protocol.APIKey, clientID string, payload []byte) uint32 {
	c.corrID++
	id := c.corrID
	clientIDBytes := []byte(clientID)
	bodyLen := 1 + 4 + 2 + len(clientIDBytes) + len(payload)
	frame := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(frame[0:], uint32(bodyLen))
	frame[4] = byte(apiKey)
	binary.BigEndian.PutUint32(frame[5:], id)
	binary.BigEndian.PutUint16(frame[9:], uint16(len(clientIDBytes)))
	copy(frame[11:], clientIDBytes)
	copy(frame[11+len(clientIDBytes):], payload)
	c.bw.Write(frame)
	c.bw.Flush()
	return id
}

func (c *testClient) recv(t *testing.T) (uint32, protocol.ErrorCode, []byte) {
	t.Helper()
	var totalLen uint32
	if err := binary.Read(c.br, binary.BigEndian, &totalLen); err != nil {
		t.Fatalf("recv length: %v", err)
	}
	body := make([]byte, totalLen)
	if _, err := c.br.Read(body); err != nil {
		t.Fatalf("recv body: %v", err)
	}
	corrID := binary.BigEndian.Uint32(body[0:])
	errCode := protocol.ErrorCode(int16(binary.BigEndian.Uint16(body[4:])))
	return corrID, errCode, body[6:]
}

func startTestServer(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	b := broker.NewBroker(1, "localhost", 0, dataDir)
	t.Cleanup(func() { b.Close() })
	h := server.NewHandler(b)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := server.NewServer(addr, h)
	go srv.ListenAndServe()
	t.Cleanup(func() { srv.Close() })
	time.Sleep(20 * time.Millisecond)
	return addr
}

func encodeCreateTopic(topic string, partitions, rf int32) []byte {
	var buf []byte
	buf = protocol.AppendString(buf, topic)
	buf = protocol.AppendInt32(buf, partitions)
	buf = protocol.AppendInt32(buf, rf)
	return buf
}

func encodeProduce(topic string, partID int32, key, value []byte) []byte {
	var buf []byte
	buf = protocol.AppendInt16(buf, 1)
	buf = protocol.AppendInt32(buf, 500)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, topic)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, partID)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendBytes(buf, key)
	buf = protocol.AppendBytes(buf, value)
	return buf
}

func encodeFetch(topic string, partID int32, offset int64, maxBytes int32) []byte {
	var buf []byte
	buf = protocol.AppendInt32(buf, 500)
	buf = protocol.AppendInt32(buf, 0)
	buf = protocol.AppendInt32(buf, 1<<20)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, topic)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, partID)
	buf = protocol.AppendInt64(buf, offset)
	buf = protocol.AppendInt32(buf, maxBytes)
	return buf
}

func encodeMetadata(topics ...string) []byte {
	var buf []byte
	buf = protocol.AppendInt32(buf, int32(len(topics)))
	for _, t := range topics {
		buf = protocol.AppendString(buf, t)
	}
	return buf
}

func encodeOffsetCommit(groupID, topic string, partID int32, offset int64) []byte {
	var buf []byte
	buf = protocol.AppendString(buf, groupID)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, topic)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, partID)
	buf = protocol.AppendInt64(buf, offset)
	buf = protocol.AppendString(buf, "")
	return buf
}

func encodeOffsetFetch(groupID, topic string, partID int32) []byte {
	var buf []byte
	buf = protocol.AppendString(buf, groupID)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, topic)
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, partID)
	return buf
}

func TestCreateTopicAndMetadata(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "test-client", encodeCreateTopic("events", 3, 1))
	if _, errCode, _ := cli.recv(t); errCode != protocol.ErrNone {
		t.Fatalf("CreateTopic error: %v", errCode)
	}

	cli.send(protocol.APIKeyMetadata, "test-client", encodeMetadata("events"))
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("Metadata error: %v", errCode)
	}

	pos := 4 // skip broker count
	brokerCount := int32(binary.BigEndian.Uint32(payload[0:]))
	for i := int32(0); i < brokerCount; i++ {
		pos += 4
		hostLen := int(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2 + hostLen + 4
	}
	topicCount := int32(binary.BigEndian.Uint32(payload[pos:]))
	pos += 4
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	pos += 2 // topic error code
	nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2
	topicName := string(payload[pos : pos+nameLen])
	pos += nameLen
	if topicName != "events" {
		t.Errorf("topic name = %q, want %q", topicName, "events")
	}
	partCount := int32(binary.BigEndian.Uint32(payload[pos:]))
	if partCount != 3 {
		t.Errorf("partition count = %d, want 3", partCount)
	}
}

func TestProduceAndFetch(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "p", encodeCreateTopic("orders", 1, 1))
	cli.recv(t)

	messages := []string{"order-1", "order-2", "order-3"}
	for _, msg := range messages {
		cli.send(protocol.APIKeyProduce, "p", encodeProduce("orders", 0, []byte("k"), []byte(msg)))
		if _, errCode, _ := cli.recv(t); errCode != protocol.ErrNone {
			t.Fatalf("Produce: %v", errCode)
		}
	}

	cli.send(protocol.APIKeyFetch, "c", encodeFetch("orders", 0, 0, 1<<20))
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("Fetch: %v", errCode)
	}

	pos := 4
	nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2 + nameLen + 4 + 4 + 2 + 8
	recCount := int32(binary.BigEndian.Uint32(payload[pos:]))
	if int(recCount) != len(messages) {
		t.Errorf("fetched %d records, want %d", recCount, len(messages))
	}
}

func TestFetchUnknownTopic(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyFetch, "c", encodeFetch("nonexistent", 0, 0, 1<<20))
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("outer error: %v", errCode)
	}
	pos := 4
	nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2 + nameLen + 4 + 4
	partErrCode := protocol.ErrorCode(int16(binary.BigEndian.Uint16(payload[pos:])))
	if partErrCode != protocol.ErrUnknownTopicPartition {
		t.Errorf("partition error = %v, want ErrUnknownTopicPartition", partErrCode)
	}
}

func TestOffsetCommitAndFetch(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic("payments", 1, 1))
	cli.recv(t)
	cli.send(protocol.APIKeyProduce, "p", encodeProduce("payments", 0, nil, []byte("pay-1")))
	cli.recv(t)

	cli.send(protocol.APIKeyOffsetCommit, "c", encodeOffsetCommit("billing", "payments", 0, 0))
	if _, errCode, _ := cli.recv(t); errCode != protocol.ErrNone {
		t.Fatalf("OffsetCommit: %v", errCode)
	}

	cli.send(protocol.APIKeyOffsetFetch, "c", encodeOffsetFetch("billing", "payments", 0))
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("OffsetFetch: %v", errCode)
	}
	pos := 4
	nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2 + nameLen + 4 + 4
	committed := int64(binary.BigEndian.Uint64(payload[pos:]))
	if committed != 0 {
		t.Errorf("committed offset = %d, want 0", committed)
	}
}

func TestOffsetFetchNoCommit(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic("logs", 1, 1))
	cli.recv(t)

	cli.send(protocol.APIKeyOffsetFetch, "c", encodeOffsetFetch("new-group", "logs", 0))
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("OffsetFetch: %v", errCode)
	}
	pos := 4
	nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2 + nameLen + 4 + 4
	committed := int64(binary.BigEndian.Uint64(payload[pos:]))
	if committed != -1 {
		t.Errorf("uncommitted offset = %d, want -1", committed)
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic("dup", 1, 1))
	cli.recv(t)

	cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic("dup", 1, 1))
	_, _, payload := cli.recv(t)
	nameLen := int(binary.BigEndian.Uint16(payload[0:]))
	innerErr := protocol.ErrorCode(int16(binary.BigEndian.Uint16(payload[2+nameLen:])))
	if innerErr != protocol.ErrTopicAlreadyExists {
		t.Errorf("inner error = %v, want ErrTopicAlreadyExists", innerErr)
	}
}

func TestCorrelationIDMirrored(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic("corr", 1, 1))
	sentID := cli.corrID
	recvID, _, _ := cli.recv(t)
	if recvID != sentID {
		t.Errorf("correlation ID: sent %d, got %d", sentID, recvID)
	}
}

func TestMultiplePartitionsProduce(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	cli.send(protocol.APIKeyCreateTopic, "p", encodeCreateTopic("multi", 3, 1))
	cli.recv(t)

	for partID := int32(0); partID < 3; partID++ {
		msg := fmt.Sprintf("msg-%d", partID)
		cli.send(protocol.APIKeyProduce, "p", encodeProduce("multi", partID, nil, []byte(msg)))
		if _, errCode, _ := cli.recv(t); errCode != protocol.ErrNone {
			t.Errorf("Produce to partition %d: %v", partID, errCode)
		}
	}

	for partID := int32(0); partID < 3; partID++ {
		cli.send(protocol.APIKeyFetch, "c", encodeFetch("multi", partID, 0, 1<<20))
		_, errCode, payload := cli.recv(t)
		if errCode != protocol.ErrNone {
			t.Errorf("Fetch partition %d: %v", partID, errCode)
			continue
		}
		pos := 4
		nameLen := int(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2 + nameLen + 4 + 4 + 2 + 8
		recCount := int32(binary.BigEndian.Uint32(payload[pos:]))
		if recCount != 1 {
			t.Errorf("partition %d: %d records, want 1", partID, recCount)
		}
	}
}

func TestMetadataAllTopics(t *testing.T) {
	addr := startTestServer(t)
	cli := newTestClient(t, addr)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		cli.send(protocol.APIKeyCreateTopic, "c", encodeCreateTopic(name, 1, 1))
		cli.recv(t)
	}

	cli.send(protocol.APIKeyMetadata, "c", encodeMetadata())
	_, errCode, payload := cli.recv(t)
	if errCode != protocol.ErrNone {
		t.Fatalf("Metadata: %v", errCode)
	}

	pos := 0
	brokerCount := int32(binary.BigEndian.Uint32(payload[pos:]))
	pos += 4
	for i := int32(0); i < brokerCount; i++ {
		pos += 4
		hostLen := int(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2 + hostLen + 4
	}
	topicCount := int32(binary.BigEndian.Uint32(payload[pos:]))
	if topicCount != 3 {
		t.Errorf("all-topics metadata count = %d, want 3", topicCount)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
