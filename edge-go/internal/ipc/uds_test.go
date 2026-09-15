package ipc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"testing"
)

func TestUDSPingPongIntegrity(t *testing.T) {
	sockPath := "/tmp/test_pingpong.sock"
	_ = os.Remove(sockPath)
	defer os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	defer ln.Close()

	testCount := 1000
	frameLen := 40
	testData := make([]byte, frameLen)
	binary.LittleEndian.PutUint16(testData[0:2], 0x534D)
	testData[2] = 0x01
	testData[3] = 0x00
	binary.LittleEndian.PutUint16(testData[4:6], 32)
	binary.LittleEndian.PutUint64(testData[8:16], 123456789)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			t.Errorf("accept error: %v", err)
			return
		}
		defer conn.Close()

		recvBuf := make([]byte, frameLen)
		for i := 0; i < testCount; i++ {
			if _, err := io.ReadFull(conn, recvBuf); err != nil {
				t.Errorf("read full error at %d: %v", i, err)
				return
			}
			if !bytes.Equal(recvBuf[:8], testData[:8]) {
				t.Errorf("packet corruption detected at index %d", i)
				return
			}
		}
	}()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	for i := 0; i < testCount; i++ {
		if _, err := conn.Write(testData); err != nil {
			t.Fatalf("write error at %d: %v", i, err)
		}
	}

	wg.Wait()
}
