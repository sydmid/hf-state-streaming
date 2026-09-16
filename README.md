<div align="center">

# High-Frequency State Streaming & Matching Engine

**A production-grade, ultra-low-latency state streaming and order matching engine designed for absolute reliability and performance.**

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go)](https://golang.org)
[![.NET Version](https://img.shields.io/badge/.NET-9.0-512BD4?style=for-the-badge&logo=dotnet)](https://dotnet.microsoft.com)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](LICENSE)

</div>

---

This framework is built for systems where **nanoseconds matter**. By decoupling the network boundary from a deterministic core engine, it achieves uncompromising throughput and near-zero latency, all while guaranteeing zero garbage collection overhead on the hot path.

## 🚀 Use Cases & Possibilities

Our architecture provides the foundation for massive-scale, high-frequency state management. It is perfectly suited for:

- **Real-Time Multiplayer Game Economies:** Power massive in-game markets, player-to-player trading, and instantaneous asset exchanges with absolute consistency.
- **High-Throughput Live Bidding/Auction Systems:** Execute thousands of concurrent bids per second with strict price-time priority and unwavering stability.
- **High-Frequency Trading (HFT):** Build cryptocurrency exchanges or financial matching systems capable of institutional-grade volume and speed.

## ⚡ Uncompromising Performance

Built with a Native-AOT .NET 9 matching core and a Go 1.22+ edge gateway, the engine communicates entirely via zero-allocation memory streams and Unix Domain Sockets.

**Under a sustained load of 100,000 orders per second:**
* **Median Latency (P50):** `35.15 µs`
* **Tail Latency (P99.9):** `77.04 µs`
* **GC Allocations on Hot Path:** `0 Bytes` (Zero Pauses)

## 📖 Deep Technical Documentation

For developers and systems engineers looking to understand the underlying mechanics, protocol schemas, and memory management strategies:

* [**Architecture Overview**](docs/ARCHITECTURE.md) - Learn about our lock-free Disruptor implementation, Go `NextReader` zero-alloc loops, and IPC boundaries.
* [**Binary Protocol Specification**](docs/PROTOCOL.md) - Discover our fixed-width, little-endian binary layout used for zero-copy deserialization.

## 🏁 Quick Start Verification

Get the environment running and verify our 0-allocation performance claims locally:

```bash
# 1. Setup IPC Sockets
make setup-uds

# 2. Run .NET Core Engine Benchmarks (Verifies 0 B allocations)
make benchmark-dotnet

# 3. Compile Native AOT Engine
make build-dotnet-aot

# 4. Run End-to-End Sustained Load Benchmark
make benchmark-e2e
```

---
<div align="center">
<i>Contributions, bug reports, and optimizations are welcome. Licensed under the MIT License.</i>
</div>
