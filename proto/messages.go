package proto

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	FrameMagic uint16 = 0x534D // "SM" in Little-Endian

	MsgTypeNewOrder         uint8 = 0x01
	MsgTypeCancelOrder      uint8 = 0x02
	MsgTypeOrderAck         uint8 = 0x03
	MsgTypeOrderExecuted    uint8 = 0x04
	MsgTypeOrderBookL2Delta uint8 = 0x05

	FlagSideBuy  uint8 = 0x00
	FlagSideSell uint8 = 0x01 // Bit 0
	FlagIOC      uint8 = 0x02 // Bit 1
	FlagPostOnly uint8 = 0x04 // Bit 2

	AckStatusAccepted uint8 = 0x00
	AckStatusRejected uint8 = 0x01
	AckStatusCanceled uint8 = 0x02

	FrameHeaderSize        = 8
	NewOrderPayloadSize    = 32
	CancelOrderPayloadSize = 16
	OrderAckPayloadSize    = 24
	OrderExecutedSize      = 32
	OrderBookL2DeltaSize   = 24
)

var (
	ErrInvalidMagic       = errors.New("invalid magic number in frame header")
	ErrBufferTooShort     = errors.New("buffer too short for expected frame payload")
	ErrUnknownMessageType = errors.New("unknown message type")
)

type FrameHeader struct {
	Magic      uint16
	MsgType    uint8
	Flags      uint8
	PayloadLen uint16
	Reserved   uint16
}

type NewOrderPayload struct {
	OrderId          uint64
	PlayerOrTraderId uint64
	Price            int64
	Quantity         uint32
	AssetId          uint32
}

type CancelOrderPayload struct {
	OrderId          uint64
	PlayerOrTraderId uint64
}

type OrderAckPayload struct {
	OrderId          uint64
	PlayerOrTraderId uint64
	Status           uint8
	Reserved         [7]byte
}

type OrderExecutedPayload struct {
	TradeId        uint64
	MakerOrderId   uint64
	TakerOrderId   uint64
	ExecutionPrice int64
}

type OrderBookL2DeltaPayload struct {
	AssetId     uint32
	Reserved1   uint32
	Price       int64
	NewQuantity uint32
	Side        uint8
	Reserved2   [3]byte
}

func CastFrameHeader(b []byte) *FrameHeader {
	if len(b) < FrameHeaderSize {
		return nil
	}
	return (*FrameHeader)(unsafe.Pointer(&b[0]))
}

func CastNewOrderPayload(b []byte) *NewOrderPayload {
	if len(b) < NewOrderPayloadSize {
		return nil
	}
	return (*NewOrderPayload)(unsafe.Pointer(&b[0]))
}

func CastCancelOrderPayload(b []byte) *CancelOrderPayload {
	if len(b) < CancelOrderPayloadSize {
		return nil
	}
	return (*CancelOrderPayload)(unsafe.Pointer(&b[0]))
}

func CastOrderAckPayload(b []byte) *OrderAckPayload {
	if len(b) < OrderAckPayloadSize {
		return nil
	}
	return (*OrderAckPayload)(unsafe.Pointer(&b[0]))
}

func CastOrderExecutedPayload(b []byte) *OrderExecutedPayload {
	if len(b) < OrderExecutedSize {
		return nil
	}
	return (*OrderExecutedPayload)(unsafe.Pointer(&b[0]))
}

func CastOrderBookL2DeltaPayload(b []byte) *OrderBookL2DeltaPayload {
	if len(b) < OrderBookL2DeltaSize {
		return nil
	}
	return (*OrderBookL2DeltaPayload)(unsafe.Pointer(&b[0]))
}

func EncodeHeader(dst []byte, msgType uint8, flags uint8, payloadLen uint16) {
	binary.LittleEndian.PutUint16(dst[0:2], FrameMagic)
	dst[2] = msgType
	dst[3] = flags
	binary.LittleEndian.PutUint16(dst[4:6], payloadLen)
	binary.LittleEndian.PutUint16(dst[6:8], 0)
}
