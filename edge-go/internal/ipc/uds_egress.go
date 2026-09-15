package ipc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/lxzan/gws"
)

const (
	DefaultEgressSocketPath = "/tmp/engine_egress.sock"
	HeaderSize              = 8
)

type EgressBroadcaster struct {
	socketPath  string
	listener    net.Listener
	subscribers sync.Map
	stopChan    chan struct{}
	wg          sync.WaitGroup
	running     atomic.Bool
	eventsRead  uint64
	bytesRead   uint64
}

func NewEgressBroadcaster(socketPath string) *EgressBroadcaster {
	if socketPath == "" {
		socketPath = DefaultEgressSocketPath
	}
	return &EgressBroadcaster{
		socketPath: socketPath,
		stopChan:   make(chan struct{}),
	}
}

func (eb *EgressBroadcaster) Subscribe(conn *gws.Conn) {
	eb.subscribers.Store(conn, struct{}{})
}

func (eb *EgressBroadcaster) Unsubscribe(conn *gws.Conn) {
	eb.subscribers.Delete(conn)
}

func (eb *EgressBroadcaster) Start() error {
	_ = os.Remove(eb.socketPath)

	ln, err := net.Listen("unix", eb.socketPath)
	if err != nil {
		return err
	}
	eb.listener = ln
	eb.running.Store(true)

	eb.wg.Add(1)
	go eb.acceptLoop()

	return nil
}

func (eb *EgressBroadcaster) acceptLoop() {
	defer eb.wg.Done()

	for eb.running.Load() {
		conn, err := eb.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || !eb.running.Load() {
				return
			}
			continue
		}

		eb.wg.Add(1)
		go eb.handleEgressStream(conn)
	}
}

func (eb *EgressBroadcaster) handleEgressStream(conn net.Conn) {
	defer eb.wg.Done()
	defer conn.Close()

	if uc, ok := conn.(*net.UnixConn); ok {
		_ = uc.SetReadBuffer(4 * 1024 * 1024)
	}

	headerBuf := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 65536)

	for eb.running.Load() {
		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			return
		}
		atomic.AddUint64(&eb.bytesRead, HeaderSize)

		magic := binary.LittleEndian.Uint16(headerBuf[0:2])
		if magic != 0x534D {
			continue
		}

		payloadLen := binary.LittleEndian.Uint16(headerBuf[4:6])
		totalFrameLen := HeaderSize + int(payloadLen)

		if int(payloadLen) > len(payloadBuf) {
			payloadBuf = make([]byte, payloadLen*2)
		}

		if _, err := io.ReadFull(conn, payloadBuf[:payloadLen]); err != nil {
			return
		}
		atomic.AddUint64(&eb.bytesRead, uint64(payloadLen))
		atomic.AddUint64(&eb.eventsRead, 1)

		fullFrame := make([]byte, totalFrameLen)
		copy(fullFrame[0:HeaderSize], headerBuf)
		copy(fullFrame[HeaderSize:], payloadBuf[:payloadLen])

		broadcaster := gws.NewBroadcaster(gws.OpcodeBinary, fullFrame)
		eb.subscribers.Range(func(key, value any) bool {
			if clientConn, ok := key.(*gws.Conn); ok {
				_ = broadcaster.Broadcast(clientConn)
			}
			return true
		})
		broadcaster.Close()
	}
}

func (eb *EgressBroadcaster) Stop() error {
	eb.running.Store(false)
	close(eb.stopChan)
	if eb.listener != nil {
		_ = eb.listener.Close()
	}
	_ = os.Remove(eb.socketPath)
	eb.wg.Wait()
	return nil
}

func (eb *EgressBroadcaster) Stats() (uint64, uint64) {
	return atomic.LoadUint64(&eb.eventsRead), atomic.LoadUint64(&eb.bytesRead)
}
