#ifndef PROTO_MESSAGES_H
#define PROTO_MESSAGES_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 1)

#define FRAME_MAGIC 0x534D // "SM" in little endian

enum MsgType {
    MSG_NEW_ORDER            = 0x01,
    MSG_CANCEL_ORDER         = 0x02,
    MSG_ORDER_ACK            = 0x03,
    MSG_ORDER_EXECUTED       = 0x04,
    MSG_ORDER_BOOK_L2_DELTA  = 0x05
};

enum OrderFlags {
    FLAG_SIDE_BUY   = 0x00,
    FLAG_SIDE_SELL  = 0x01, // Bit 0
    FLAG_IOC        = 0x02, // Bit 1
    FLAG_POST_ONLY  = 0x04  // Bit 2
};

enum AckStatus {
    ACK_STATUS_ACCEPTED = 0x00,
    ACK_STATUS_REJECTED = 0x01,
    ACK_STATUS_CANCELED = 0x02
};

// 8 bytes fixed header
typedef struct {
    uint16_t magic;       // 0x534D ("SM")
    uint8_t  msg_type;    // MsgType enum
    uint8_t  flags;       // Bit 0=Side, Bit 1=IOC, Bit 2=PostOnly
    uint16_t payload_len; // Length of payload following header
    uint16_t reserved;    // Alignment padding
} FrameHeader;

// 32 bytes NewOrder payload
typedef struct {
    uint64_t order_id;            // Unique order identifier
    uint64_t player_or_trader_id; // Client / Agent ID
    int64_t  price;               // Fixed-point 4 decimals (e.g. 100.2500 -> 1002500)
    uint32_t quantity;            // Order quantity
    uint32_t asset_id;            // Instrument / Asset ID
} NewOrderPayload;

// 16 bytes CancelOrder payload
typedef struct {
    uint64_t order_id;            // Order to cancel
    uint64_t player_or_trader_id; // Owner identifier
} CancelOrderPayload;

// 24 bytes OrderAck payload
typedef struct {
    uint64_t order_id;
    uint64_t player_or_trader_id;
    uint8_t  status;              // AckStatus
    uint8_t  reserved[7];         // Padding
} OrderAckPayload;

// 32 bytes OrderExecuted payload
typedef struct {
    uint64_t trade_id;
    uint64_t maker_order_id;
    uint64_t taker_order_id;
    int64_t  execution_price;
} OrderExecutedPayload;

// 24 bytes OrderBookLevel2Delta payload
typedef struct {
    uint32_t asset_id;
    uint32_t reserved1;
    int64_t  price;
    uint32_t new_quantity;
    uint8_t  side;                // 0 = Bid, 1 = Ask
    uint8_t  reserved2[3];
} OrderBookL2DeltaPayload;

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif // PROTO_MESSAGES_H
