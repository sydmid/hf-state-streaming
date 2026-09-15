package ipc

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultIngressSocketPath = "/tmp/engine_ingress.sock"
	DefaultBufferSize        = 65536
	FlushIntervalNs          = 10000
)

var (
	ErrNotConnected = errors.New("ipc client not connected")
	ErrBufferFull   = errors.New("ipc buffer full")
)

type UDSClient struct {
	socketPath string
	conn       net.Conn
	mu         sync.Mutex
	bufPool    *sync.Pool

	queue      chan []byte
	stopChan   chan struct{}
	wg         sync.WaitGroup
	bytesSent  uint64
	framesSent uint64
	connected  atomic.Bool
}

func NewUDSClient(socketPath string) *UDSClient {
	if socketPath == "" {
		socketPath = DefaultIngressSocketPath
	}
	return &UDSClient{
		socketPath: socketPath,
		bufPool: &sync.Pool{
			New: func() any {
				b := make([]byte, DefaultBufferSize)
				return &b
			},
		},
		queue:    make(chan []byte, 16384),
		stopChan: make(chan struct{}),
	}
}

func (c *UDSClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("failed to dial unix socket %s: %w", c.socketPath, err)
	}

	if uc, ok := conn.(*net.UnixConn); ok {
		_ = uc.SetWriteBuffer(4 * 1024 * 1024)
	}

	c.conn = conn
	c.connected.Store(true)

	c.wg.Add(1)
	go c.batchFlushLoop()

	return nil
}

func (c *UDSClient) ForwardFrame(frame []byte) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	bufPtr := c.bufPool.Get().(*[]byte)
	buf := (*bufPtr)[:len(frame)]
	copy(buf, frame)

	select {
	case c.queue <- buf:
		return nil
	default:
		c.bufPool.Put(bufPtr)
		return ErrBufferFull
	}
}

func (c *UDSClient) WriteDirect(frame []byte) (int, error) {
	if !c.connected.Load() || c.conn == nil {
		return 0, ErrNotConnected
	}
	n, err := c.conn.Write(frame)
	if err == nil {
		atomic.AddUint64(&c.bytesSent, uint64(n))
		atomic.AddUint64(&c.framesSent, 1)
	}
	return n, err
}

func (c *UDSClient) batchFlushLoop() {
	defer c.wg.Done()

	batchBuf := make([]byte, 0, DefaultBufferSize)
	recycled := make([]*[]byte, 0, 512)
	ticker := time.NewTicker(time.Duration(FlushIntervalNs) * time.Nanosecond)
	defer ticker.Stop()

	flush := func() {
		if len(batchBuf) == 0 {
			return
		}
		if c.conn != nil {
			n, err := c.conn.Write(batchBuf)
			if err == nil {
				atomic.AddUint64(&c.bytesSent, uint64(n))
			} else {
				c.connected.Store(false)
			}
		}
		batchBuf = batchBuf[:0]
		for _, ptr := range recycled {
			c.bufPool.Put(ptr)
		}
		recycled = recycled[:0]
	}

	for {
		select {
		case <-c.stopChan:
			flush()
			return
		case item := <-c.queue:
			if len(batchBuf)+len(item) > cap(batchBuf) {
				flush()
			}
			batchBuf = append(batchBuf, item...)
			atomic.AddUint64(&c.framesSent, 1)
			recycled = append(recycled, &item)

		drainLoop:
			for len(batchBuf) < cap(batchBuf)/2 {
				select {
				case next := <-c.queue:
					if len(batchBuf)+len(next) > cap(batchBuf) {
						flush()
					}
					batchBuf = append(batchBuf, next...)
					atomic.AddUint64(&c.framesSent, 1)
					recycled = append(recycled, &next)
				default:
					break drainLoop
				}
			}

			if len(batchBuf) >= cap(batchBuf)/2 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (c *UDSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connected.Store(false)
	close(c.stopChan)
	c.wg.Wait()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *UDSClient) Stats() (uint64, uint64) {
	return atomic.LoadUint64(&c.framesSent), atomic.LoadUint64(&c.bytesSent)
}
