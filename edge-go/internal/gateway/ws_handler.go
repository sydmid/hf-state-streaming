package gateway

import (
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"

	"hf-state-streaming/edge-go/internal/ipc"

	"github.com/lxzan/gws"
)

const (
	ChunkBufferSize = 4096
)

type GatewayHandler struct {
	gws.BuiltinEventHandler
	ipcClient       *ipc.UDSClient
	egressBroadcast *ipc.EgressBroadcaster
	chunkPool       *sync.Pool
	activeConns     int64
	inboundFrames   uint64
	inboundBytes    uint64
	dropCount       uint64
}

func NewGatewayHandler(ipcClient *ipc.UDSClient, egressBroadcast *ipc.EgressBroadcaster) *GatewayHandler {
	return &GatewayHandler{
		ipcClient:       ipcClient,
		egressBroadcast: egressBroadcast,
		chunkPool: &sync.Pool{
			New: func() any {
				b := make([]byte, ChunkBufferSize)
				return &b
			},
		},
	}
}

func (h *GatewayHandler) OnOpen(socket *gws.Conn) {
	atomic.AddInt64(&h.activeConns, 1)
	if h.egressBroadcast != nil {
		h.egressBroadcast.Subscribe(socket)
	}

	go h.runCustomReader(socket)
}

func (h *GatewayHandler) OnClose(socket *gws.Conn, err error) {
	atomic.AddInt64(&h.activeConns, -1)
	if h.egressBroadcast != nil {
		h.egressBroadcast.Unsubscribe(socket)
	}
}

func (h *GatewayHandler) OnError(socket *gws.Conn, err error) {
	log.Printf("[gateway] socket error: %v", err)
}

func (h *GatewayHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
}

func (h *GatewayHandler) runCustomReader(socket *gws.Conn) {
	bufPtr := h.chunkPool.Get().(*[]byte)
	buf := *bufPtr
	defer h.chunkPool.Put(bufPtr)

	for {
		opcode, reader, err := socket.NextReader()
		if err != nil {
			return
		}

		if opcode != gws.OpcodeBinary {
			_, _ = io.CopyBuffer(io.Discard, reader, buf)
			continue
		}

		for {
			n, rErr := reader.Read(buf)
			if n > 0 {
				atomic.AddUint64(&h.inboundBytes, uint64(n))
				atomic.AddUint64(&h.inboundFrames, 1)

				if err := h.ipcClient.ForwardFrame(buf[:n]); err != nil {
					atomic.AddUint64(&h.dropCount, 1)
				}
			}

			if rErr != nil {
				if errors.Is(rErr, io.EOF) {
					break
				}
				return
			}
		}
	}
}

func (h *GatewayHandler) Stats() (int64, uint64, uint64, uint64) {
	return atomic.LoadInt64(&h.activeConns),
		atomic.LoadUint64(&h.inboundFrames),
		atomic.LoadUint64(&h.inboundBytes),
		atomic.LoadUint64(&h.dropCount)
}
