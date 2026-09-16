# Architecture

The architecture decouples the network boundary from core deterministic state execution across two local processes connected via Unix Domain Sockets:
1. **Edge Ingestion & Egress Service (Go 1.22+)**: Leverages [`github.com/lxzan/gws`](https://github.com/lxzan/gws) with zero-allocation `NextReader()` streaming, sequential per-socket event dispatching (`ParallelEnabled: false`), and vectorized single-compression market broadcast (`gws.Broadcaster`).
2. **Core Matching Engine (.NET 9 / C#)**: A headless, Native-AOT-ready daemon consuming IPC frames via `System.IO.Pipelines`, arbitrating events across an LMAX Disruptor-inspired lock-free cache-line aligned SPSC ring buffer, and maintaining an unmanaged price-time priority Limit Order Book with **0 Gen 0/1/2 GC allocations** during active matching.

## Architectural Overview

```text
                      [ High-Frequency Clients / Market Makers ]
                                     │        ▲
                Inbound Binary Order │        │ Outbound L2 Diffs & Executions
                     WebSocket (gws) │        │ gws.Broadcaster (Vectorized Fanout)
                                     ▼        │
                     ┌─────────────────────────────────────────┐
                     │          Go 1.22+ Edge Gateway          │
                     │  - socket.NextReader() stream parsing   │
                     │  - Custom sync.Pool chunk allocators    │
                     │  - ParallelEnabled: false (0 ctx-switch)│
                     └───────────────────┬─────────────────────┘
                                         │ /tmp/engine_ingress.sock (batched UDS)
                                         ▼
                     ┌─────────────────────────────────────────┐
                     │    .NET 9 System.IO.Pipelines Reader    │
                     │  - Zero-copy ReadOnlySequence<byte>     │
                     │  - Span-based frame parsing             │
                     └───────────────────┬─────────────────────┘
                                         │ SPSC Lock-Free RingBuffer (Disruptor)
                                         │ (64-byte cache-line padded sequences)
                                         ▼
                     ┌─────────────────────────────────────────┐
                     │        Pinned Core Matching Loop        │
                     │  - Price-Time Priority Limit Order Book │
                     │  - NativeMemory flat array order pools  │
                     │  - O(1) Fibonacci hash index for cancels│
                     │  - 0 Gen 0/1/2 allocations on hot path  │
                     └───────────────────┬─────────────────────┘
                                         │ /tmp/engine_egress.sock (UDS)
                                         ▼
                     ┌─────────────────────────────────────────┐
                     │        Go Vectorized Egress Fanout      │
                     └─────────────────────────────────────────┘
```

## Component Engineering Highlights

### Go Edge Ingestion & Fanout Tier (`/edge-go`)
* **`socket.NextReader()` Zero-Allocation Loop**: Ingests WebSocket payload fragments directly into pre-allocated, pooled 4 KB memory chunks (`sync.Pool`), completely avoiding `io.ReadAll()` heap allocations.
* **Non-Blocking UDS Batching**: Ingress frames are queued into an asynchronous micro-batching forwarder (`internal/ipc/uds_client.go`) writing directly to `/tmp/engine_ingress.sock`.
* **Sequential Event Dispatching**: Server option `ParallelEnabled: false` ensures per-socket execution remains strictly sequential, preventing thread thrashing and cache evictions on single connections.
* **Vectorized Market Data Egress**: Subscribes to `/tmp/engine_egress.sock` and leverages `gws.NewBroadcaster` to fan out trade ticks and L2 diffs across all subscribers with single-pass frame encoding.

### .NET 9 Core Matching Engine (`/engine-dotnet`)
* **`System.IO.Pipelines` IPC Server**: High-performance socket listener on `/tmp/engine_ingress.sock` parsing frames directly out of `ReadOnlySequence<byte>` via `Unsafe.ReadUnaligned<T>`, advancing stream pointers with zero buffer reallocations.
* **Lock-Free Cache-Line Padded SPSC Disruptor**: Head and tail cursors use explicit 64-byte padding (`[StructLayout(LayoutKind.Explicit, Size = 64)]`) to prevent CPU false sharing. Circular indexing uses bitwise power-of-two masking (`index & (Capacity - 1)`).
* **Flat Unmanaged Order Book**:
  * Backed by `NativeMemory.AllocZeroed` flat pools (pre-sized for 256K active orders and 64K price levels).
  * Price levels organized as intrusive doubly linked lists (Bids descending, Asks ascending).
  * Order cancellation index implemented as an unmanaged open-addressing hash table with Fibonacci multiplication hashing for $O(1)$ constant-time lookup.
  * Native AOT compiled (`<PublishAot>true</PublishAot>`), generating a fully self-contained machine binary without runtime JIT pauses.
