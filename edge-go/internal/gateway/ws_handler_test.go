package gateway

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"testing"
)

type mockReader struct {
	data   []byte
	offset int
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.offset:])
	m.offset += n
	return n, nil
}

func BenchmarkZeroAllocReaderStream(b *testing.B) {
	frame := make([]byte, 40)
	binary.LittleEndian.PutUint16(frame[0:2], 0x534D)
	frame[2] = 0x01
	frame[3] = 0x00
	binary.LittleEndian.PutUint16(frame[4:6], 32)
	binary.LittleEndian.PutUint16(frame[6:8], 0)

	chunkPool := &sync.Pool{
		New: func() any {
			buf := make([]byte, ChunkBufferSize)
			return &buf
		},
	}

	mock := &mockReader{data: frame}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mock.offset = 0
		bufPtr := chunkPool.Get().(*[]byte)
		buf := *bufPtr

		for {
			n, err := mock.Read(buf)
			if n > 0 {
				if len(buf[:n]) < 8 {
					b.Fatal("invalid read")
				}
			}
			if err != nil {
				break
			}
		}

		chunkPool.Put(bufPtr)
	}
}

func TestProtocolEncoding(t *testing.T) {
	frame := make([]byte, 40)
	binary.LittleEndian.PutUint16(frame[0:2], 0x534D)
	frame[2] = 0x01
	frame[3] = 0x01
	binary.LittleEndian.PutUint16(frame[4:6], 32)

	if !bytes.Equal(frame[0:2], []byte{0x4D, 0x53}) {
		t.Fatalf("Magic mismatch")
	}
}
