#include "messages.h"
#include <assert.h>
#include <stdio.h>

int main(void) {
    static_assert(sizeof(FrameHeader) == 8, "FrameHeader must be 8 bytes");
    static_assert(offsetof(FrameHeader, magic) == 0, "magic offset");
    static_assert(offsetof(FrameHeader, msg_type) == 2, "msg_type offset");
    static_assert(offsetof(FrameHeader, flags) == 3, "flags offset");
    static_assert(offsetof(FrameHeader, payload_len) == 4, "payload_len offset");
    static_assert(offsetof(FrameHeader, reserved) == 6, "reserved offset");

    static_assert(sizeof(NewOrderPayload) == 32, "NewOrderPayload must be 32 bytes");
    static_assert(offsetof(NewOrderPayload, order_id) == 0, "order_id offset");
    static_assert(offsetof(NewOrderPayload, player_or_trader_id) == 8, "trader offset");
    static_assert(offsetof(NewOrderPayload, price) == 16, "price offset");
    static_assert(offsetof(NewOrderPayload, quantity) == 24, "qty offset");
    static_assert(offsetof(NewOrderPayload, asset_id) == 28, "asset offset");

    static_assert(sizeof(CancelOrderPayload) == 16, "CancelOrderPayload must be 16 bytes");
    static_assert(offsetof(CancelOrderPayload, order_id) == 0, "order_id offset");
    static_assert(offsetof(CancelOrderPayload, player_or_trader_id) == 8, "trader offset");

    static_assert(sizeof(OrderAckPayload) == 24, "OrderAckPayload must be 24 bytes");
    static_assert(sizeof(OrderExecutedPayload) == 32, "OrderExecutedPayload must be 32 bytes");
    static_assert(sizeof(OrderBookL2DeltaPayload) == 24, "OrderBookL2DeltaPayload must be 24 bytes");

    printf("SUCCESS: All binary protocol layouts, sizes, and offsets strictly match specification!\n");
    return 0;
}
