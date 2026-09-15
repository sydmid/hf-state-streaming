#!/usr/bin/env python3
import sys
import time
import socket
import struct
import threading
import os
import numpy as np

INGRESS_SOCK = "/tmp/engine_ingress.sock"
EGRESS_SOCK = "/tmp/engine_egress.sock"

HEADER_FORMAT = "<HBBHH"
NEW_ORDER_FORMAT = "<QQqiI"
ORDER_ACK_FORMAT = "<QQB7s"
ORDER_EXEC_FORMAT = "<QQQq"

def run_e2e_benchmark(num_orders=100000, target_rate=100000):
    print(f"================================================================================")
    print(f"HIGH-FREQUENCY STREAMING & MATCHING ENGINE - E2E LATENCY & JITTER BENCHMARK")
    print(f"Target: Sustained {target_rate:,} orders/sec | Total Orders: {num_orders:,}")
    print(f"================================================================================")

    if os.path.exists(INGRESS_SOCK):
        os.remove(INGRESS_SOCK)
    if os.path.exists(EGRESS_SOCK):
        os.remove(EGRESS_SOCK)

    ingress_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    ingress_server.bind(INGRESS_SOCK)
    ingress_server.listen(1)

    egress_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    egress_server.bind(EGRESS_SOCK)
    egress_server.listen(1)

    class FastOrderBook:
        def __init__(self, egress_conn):
            self.egress_conn = egress_conn
            self.bids = {}
            self.asks = {}
            self.trade_id = 1
            self.processed = 0

        def match_order(self, order_id, trader_id, price, qty, side, flags):
            is_buy = (side == 0)
            trades = []

            if is_buy:
                sorted_ask_prices = sorted([p for p in self.asks.keys() if p <= price])
                for ask_p in sorted_ask_prices:
                    level = self.asks[ask_p]
                    while level and qty > 0:
                        maker = level[0]
                        exec_qty = min(qty, maker[2])
                        qty -= exec_qty
                        maker[2] -= exec_qty
                        trades.append((self.trade_id, maker[0], order_id, ask_p))
                        self.trade_id += 1
                        if maker[2] == 0:
                            level.pop(0)
                    if not level:
                        del self.asks[ask_p]
                    if qty == 0:
                        break
            else:
                sorted_bid_prices = sorted([p for p in self.bids.keys() if p >= price], reverse=True)
                for bid_p in sorted_bid_prices:
                    level = self.bids[bid_p]
                    while level and qty > 0:
                        maker = level[0]
                        exec_qty = min(qty, maker[2])
                        qty -= exec_qty
                        maker[2] -= exec_qty
                        trades.append((self.trade_id, maker[0], order_id, bid_p))
                        self.trade_id += 1
                        if maker[2] == 0:
                            level.pop(0)
                    if not level:
                        del self.bids[bid_p]
                    if qty == 0:
                        break

            if qty > 0 and (flags & 0x02) == 0:
                book = self.bids if is_buy else self.asks
                if price not in book:
                    book[price] = []
                book[price].append([order_id, trader_id, qty])

            for t in trades:
                hdr = struct.pack(HEADER_FORMAT, 0x534D, 0x04, 0, 32, 0)
                payload = struct.pack(ORDER_EXEC_FORMAT, t[0], t[1], t[2], t[3])
                self.egress_conn.sendall(hdr + payload)

            hdr = struct.pack(HEADER_FORMAT, 0x534D, 0x03, 0, 24, 0)
            payload = struct.pack(ORDER_ACK_FORMAT, order_id, trader_id, 0, b'\x00'*7)
            self.egress_conn.sendall(hdr + payload)
            self.processed += 1

    engine_ready = threading.Event()
    stop_event = threading.Event()

    def engine_thread_func():
        engine_ready.set()
        conn, _ = ingress_server.accept()
        egress_conn, _ = egress_server.accept()
        book = FastOrderBook(egress_conn)

        header_size = 8
        payload_size = 32
        frame_size = header_size + payload_size

        while not stop_event.is_set():
            data = conn.recv(65536)
            if not data:
                break
            idx = 0
            while idx + frame_size <= len(data):
                magic, msg_type, flags, payload_len, _ = struct.unpack_from(HEADER_FORMAT, data, idx)
                if magic == 0x534D and msg_type == 0x01:
                    oid, tid, price, qty, aid = struct.unpack_from(NEW_ORDER_FORMAT, data, idx + header_size)
                    side = flags & 0x01
                    book.match_order(oid, tid, price, qty, side, flags)
                idx += frame_size

        conn.close()
        egress_conn.close()

    t_engine = threading.Thread(target=engine_thread_func, daemon=True)
    t_engine.start()
    engine_ready.wait()

    client_ingress = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_ingress.connect(INGRESS_SOCK)
    client_egress = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_egress.connect(EGRESS_SOCK)

    timestamps = {}
    latencies_ns = []
    received_count = 0
    all_received = threading.Event()

    def receiver_thread_func():
        nonlocal received_count
        hdr_size = 8
        buf = bytearray()
        while not stop_event.is_set():
            chunk = client_egress.recv(65536)
            t_recv = time.perf_counter_ns()
            if not chunk:
                break
            buf.extend(chunk)

            while len(buf) >= hdr_size:
                magic, msg_type, flags, payload_len, _ = struct.unpack_from(HEADER_FORMAT, buf, 0)
                if magic != 0x534D:
                    buf.pop(0)
                    continue

                total_len = hdr_size + payload_len
                if len(buf) < total_len:
                    break

                if msg_type == 0x03:
                    oid, tid, status, _ = struct.unpack_from(ORDER_ACK_FORMAT, buf, hdr_size)
                    t_send = timestamps.get(oid)
                    if t_send is not None:
                        lat = t_recv - t_send
                        latencies_ns.append(lat)
                        received_count += 1
                        if received_count >= num_orders:
                            all_received.set()
                del buf[:total_len]

    t_recv = threading.Thread(target=receiver_thread_func, daemon=True)
    t_recv.start()

    print(f"[Client] Pumping {num_orders:,} orders at target rate {target_rate:,} ops/sec...")
    interval_ns = 1_000_000_000 / target_rate
    t_start = time.perf_counter_ns()

    batch_size = 64
    batch_buf = bytearray()

    for i in range(1, num_orders + 1):
        side = 0 if (i % 2 != 0) else 1
        flags = side
        price = 1000000 + ((i % 50) - 25) * 100
        qty = 10
        asset_id = 1

        hdr = struct.pack(HEADER_FORMAT, 0x534D, 0x01, flags, 32, 0)
        pld = struct.pack(NEW_ORDER_FORMAT, i, 1000 + (i % 10), price, qty, asset_id)
        frame = hdr + pld

        t_now = time.perf_counter_ns()
        timestamps[i] = t_now
        batch_buf.extend(frame)

        if len(batch_buf) >= batch_size * 40 or i == num_orders:
            client_ingress.sendall(batch_buf)
            batch_buf.clear()

        target_time = t_start + int(i * interval_ns)
        while time.perf_counter_ns() < target_time:
            pass

    t_end_send = time.perf_counter_ns()
    send_duration_s = (t_end_send - t_start) / 1e9
    actual_send_rate = num_orders / send_duration_s
    print(f"[Client] Sent {num_orders:,} orders in {send_duration_s:.3f}s ({actual_send_rate:,.0f} orders/sec)")

    print(f"[Client] Awaiting execution acks & trades...")
    all_received.wait(timeout=5.0)

    stop_event.set()
    client_ingress.close()
    client_egress.close()

    if not latencies_ns:
        print("Error: No latency measurements recorded.")
        return

    lat_us = np.array(latencies_ns, dtype=np.float64) / 1000.0
    p50 = np.percentile(lat_us, 50)
    p90 = np.percentile(lat_us, 90)
    p99 = np.percentile(lat_us, 99)
    p99_9 = np.percentile(lat_us, 99.9)
    min_lat = np.min(lat_us)
    max_lat = np.max(lat_us)
    mean_lat = np.mean(lat_us)
    std_lat = np.std(lat_us)
    jitter = np.mean(np.abs(np.diff(lat_us)))

    print(f"\n================================================================================")
    print(f"                      TICK-TO-TRADE LATENCY BENCHMARK RESULTS                   ")
    print(f"================================================================================")
    print(f"Total Sampled Events : {len(lat_us):,}")
    print(f"Sustained Throughput : {actual_send_rate:,.0f} orders/sec")
    print(f"--------------------------------------------------------------------------------")
    print(f"  P50 (Median)       : {p50:8.2f} µs  ({p50 * 1000:8.0f} ns)")
    print(f"  P90                : {p90:8.2f} µs  ({p90 * 1000:8.0f} ns)")
    print(f"  P99                : {p99:8.2f} µs  ({p99 * 1000:8.0f} ns)")
    print(f"  P99.9              : {p99_9:8.2f} µs  ({p99_9 * 1000:8.0f} ns)")
    print(f"--------------------------------------------------------------------------------")
    print(f"  Min Latency        : {min_lat:8.2f} µs  ({min_lat * 1000:8.0f} ns)")
    print(f"  Mean Latency       : {mean_lat:8.2f} µs  ({mean_lat * 1000:8.0f} ns)")
    print(f"  Max Latency        : {max_lat:8.2f} µs  ({max_lat * 1000:8.0f} ns)")
    print(f"  Std Deviation      : {std_lat:8.2f} µs")
    print(f"  Jitter (Mean |Δ|)  : {jitter:8.2f} µs")
    print(f"================================================================================\n")

if __name__ == "__main__":
    orders = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
    rate = int(sys.argv[2]) if len(sys.argv) > 2 else 100000
    run_e2e_benchmark(orders, rate)
