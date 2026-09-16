# Binary Protocol Specification

To prevent serialization overhead and data translation bottlenecks, all communication between edge gateways, IPC boundaries, and the core engine uses a fixed-width, little-endian binary layout (`Pack = 1`).

## 1. Frame Header (8 Bytes Fixed)

| Field | Offset | Size | Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `Magic` | 0 | 2 bytes | `uint16` | Magic identifier: `0x534D` (`"SM"`) |
| `MsgType` | 2 | 1 byte | `uint8` | `0x01`=NewOrder, `0x02`=CancelOrder, `0x03`=OrderAck, `0x04`=OrderExecuted, `0x05`=L2Delta |
| `Flags` | 3 | 1 byte | `uint8` | Bit 0: Side (`0`=Bid, `1`=Ask), Bit 1: `IOC`, Bit 2: `PostOnly` |
| `PayloadLen`| 4 | 2 bytes | `uint16` | Length of payload immediately following header |
| `Reserved` | 6 | 2 bytes | `uint16` | 16-bit alignment padding |

## 2. Message Payloads

* **`NewOrderPayload` (32 Bytes)**:
  * `OrderId` (`uint64`, 8B): Monotonically increasing unique order ID
  * `PlayerOrTraderId` (`uint64`, 8B): Authenticated participant ID
  * `Price` (`int64`, 8B): Fixed-point price scaled to 4 decimals (e.g. `100.2500` = `1002500`)
  * `Quantity` (`uint32`, 4B): Order quantity
  * `AssetId` (`uint32`, 4B): Instrument / Currency / Asset identifier
* **`CancelOrderPayload` (16 Bytes)**:
  * `OrderId` (`uint64`, 8B): Order ID to cancel
  * `PlayerOrTraderId` (`uint64`, 8B): Authenticated trader/owner ID
* **`OrderAckPayload` (24 Bytes)**:
  * `OrderId` (`uint64`, 8B): Target Order ID
  * `PlayerOrTraderId` (`uint64`, 8B): Target Trader ID
  * `Status` (`uint8`, 1B): `0x00`=Accepted, `0x01`=Rejected, `0x02`=Canceled
  * `Reserved` (`uint8[7]`, 7B): Struct alignment padding
* **`OrderExecutedPayload` (32 Bytes)**:
  * `TradeId` (`uint64`, 8B): Global trade execution ID
  * `MakerOrderId` (`uint64`, 8B): Passive resting order ID
  * `TakerOrderId` (`uint64`, 8B): Aggressive crossing order ID
  * `ExecutionPrice` (`int64`, 8B): Match execution price (fixed-point 4 decimals)
* **`OrderBookL2DeltaPayload` (24 Bytes)**:
  * `AssetId` (`uint32`, 4B): Instrument ID
  * `Reserved1` (`uint32`, 4B): Padding
  * `Price` (`int64`, 8B): Affected price level
  * `NewQuantity` (`uint32`, 4B): Aggregate aggregate level depth (`0` indicates depleted level)
  * `Side` (`uint8`, 1B): `0`=Bid, `1`=Ask
  * `Reserved2` (`uint8[3]`, 3B): Padding
