package ingest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	address := "127.0.0.1:9000" // TCP relay server

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("failed to connect to relay: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// prepare the payload to send
	payload := []byte("hello from client")
	length := uint32(len(payload))

	// build message buffer: [4 bytes length][payload]
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, length); err != nil {
		fmt.Printf("failed to write message length: %v\n", err)
		return
	}
	buf.Write(payload)

	// set a write deadline and send the message
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("failed to write message: %v\n", err)
		return
	}

	// wait for ACK (1 byte)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ack := make([]byte, 1)
	_, err = conn.Read(ack)
	if err != nil {
		fmt.Printf("failed to read ACK: %v\n", err)
		return
	}

	if ack[0] == 0x00 {
		fmt.Println("ACK received: message accepted")
	} else {
		fmt.Printf("unexpected ACK value: 0x%02X\n", ack[0])
	}
}
