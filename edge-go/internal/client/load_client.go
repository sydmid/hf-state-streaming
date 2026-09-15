package client

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
)

type LatencySample struct {
	SendTimeNano int64
	RecvTimeNano int64
}

type LoadClient struct {
	targetURL       string
	conn            *gws.Conn
	numOrders       int
	orderRate       int
	sentCount       uint64
	recvCount       uint64
	latencies       []int64
	latencyMu       sync.Mutex
	orderTimestamps sync.Map
	doneChan        chan struct{}
}

func NewLoadClient(wsURL string, numOrders int, orderRate int) *LoadClient {
	return &LoadClient{
		targetURL: wsURL,
		numOrders: numOrders,
		orderRate: orderRate,
		latencies: make([]int64, 0, numOrders),
		doneChan:  make(chan struct{}),
	}
}

type clientHandler struct {
	gws.BuiltinEventHandler
	lc *LoadClient
}

func (h *clientHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()
	data := message.Bytes()
	if len(data) < 8 {
		return
	}
	recvTime := time.Now().UnixNano()
	atomic.AddUint64(&h.lc.recvCount, 1)

	msgType := data[2]
	if msgType == 0x03 {
		if len(data) >= 8+24 {
			orderId := binary.LittleEndian.Uint64(data[8 : 8+8])
			if sendVal, ok := h.lc.orderTimestamps.Load(orderId); ok {
				latency := recvTime - sendVal.(int64)
				h.lc.recordLatency(latency)
			}
		}
	} else if msgType == 0x04 {
		if len(data) >= 8+32 {
			takerOrderId := binary.LittleEndian.Uint64(data[8+16 : 8+24])
			if sendVal, ok := h.lc.orderTimestamps.Load(takerOrderId); ok {
				latency := recvTime - sendVal.(int64)
				h.lc.recordLatency(latency)
			}
		}
	}
}

func (lc *LoadClient) recordLatency(latNs int64) {
	lc.latencyMu.Lock()
	lc.latencies = append(lc.latencies, latNs)
	if len(lc.latencies) >= lc.numOrders {
		select {
		case <-lc.doneChan:
		default:
			close(lc.doneChan)
		}
	}
	lc.latencyMu.Unlock()
}

func (lc *LoadClient) Run() error {
	u, err := url.Parse(lc.targetURL)
	if err != nil {
		return err
	}

	handler := &clientHandler{lc: lc}
	conn, _, err := gws.NewClient(handler, &gws.ClientOption{
		Addr:          u.Host,
		RequestHeader: http.Header{},
	})
	if err != nil {
		return fmt.Errorf("client dial error: %w", err)
	}
	lc.conn = conn
	go conn.ReadLoop()

	log.Printf("[load_client] Connected to %s. Pumping %d orders at target rate %d/sec...",
		lc.targetURL, lc.numOrders, lc.orderRate)

	frameBuf := make([]byte, 8+32)
	binary.LittleEndian.PutUint16(frameBuf[0:2], 0x534D)
	frameBuf[2] = 0x01
	binary.LittleEndian.PutUint16(frameBuf[4:6], 32)
	binary.LittleEndian.PutUint16(frameBuf[6:8], 0)

	intervalNs := int64(time.Second) / int64(lc.orderRate)
	nextSend := time.Now().UnixNano()

	for i := 1; i <= lc.numOrders; i++ {
		orderId := uint64(i)
		side := byte(0)
		if i%2 == 0 {
			side = byte(1)
		}
		frameBuf[3] = side

		binary.LittleEndian.PutUint64(frameBuf[8:16], orderId)
		binary.LittleEndian.PutUint64(frameBuf[16:24], uint64(1000+i%50))
		price := int64(1000000 + (i%20)*1000)
		binary.LittleEndian.PutUint64(frameBuf[24:32], uint64(price))
		binary.LittleEndian.PutUint32(frameBuf[32:36], uint32(10))
		binary.LittleEndian.PutUint32(frameBuf[36:40], uint32(1))

		now := time.Now().UnixNano()
		for now < nextSend {
			now = time.Now().UnixNano()
		}
		nextSend += intervalNs

		lc.orderTimestamps.Store(orderId, now)
		_ = lc.conn.WriteMessage(gws.OpcodeBinary, frameBuf)
		atomic.AddUint64(&lc.sentCount, 1)
	}

	log.Printf("[load_client] Sent %d orders. Awaiting ACKs/Trades...", lc.numOrders)
	select {
	case <-lc.doneChan:
		log.Printf("[load_client] Completed receiving %d samples.", len(lc.latencies))
	case <-time.After(5 * time.Second):
		log.Printf("[load_client] Timeout waiting for responses. Total received: %d", atomic.LoadUint64(&lc.recvCount))
	}

	return nil
}
