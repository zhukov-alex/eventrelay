package acceptance_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type Event struct {
	Event string `json:"event"`
	Data  string `json:"data"`
}

func TestRelayEndToEnd(t *testing.T) {
	const count = 100
	packet := makeTestPacket(t)

	for i := 0; i < count; i++ {
		conn, err := net.Dial("tcp", "localhost:9000")
		require.NoError(t, err)

		_, err = conn.Write(packet)
		require.NoError(t, err)

		ack := make([]byte, 1)
		_, err = conn.Read(ack)
		require.NoError(t, err)
		require.Equal(t, byte(0x00), ack[0])

		_ = conn.Close()
	}

	time.Sleep(1 * time.Second)

	msgs := readFromKafka(t, "events", count)
	require.Len(t, msgs, count)

	for _, raw := range msgs {
		var evt Event
		err := json.Unmarshal([]byte(raw), &evt)
		require.NoError(t, err)
		require.Equal(t, "test", evt.Event)
		require.Equal(t, "123", evt.Data)
	}
}

func makeTestPacket(t *testing.T) []byte {
	msg := Event{Event: "test", Data: "123"}
	data, err := json.Marshal(msg)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = binary.Write(&buf, binary.LittleEndian, uint32(len(data)))
	require.NoError(t, err)

	_, err = buf.Write(data)
	require.NoError(t, err)

	return buf.Bytes()
}

func readFromKafka(t *testing.T, topic string, expected int) []string {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		GroupID:  "acceptance-test-group",
		MaxWait:  1 * time.Second,
		MinBytes: 1,
		MaxBytes: 1_000_000,
	})
	defer r.Close()

	var results []string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for len(results) < expected {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("failed to read from kafka: %v", err)
		}
		results = append(results, string(m.Value))
	}

	return results
}
