using System;
using System.Diagnostics;
using System.IO;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using HfEngine.Disruptor;
using HfEngine.Ipc;
using HfEngine.Matching;
using HfEngine.Protocol;

namespace HfEngine
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("[Engine.Core] Booting .NET 9 Ultra-Low Latency Matching Engine (Native AOT Target)...");
            Console.WriteLine($"[Engine.Core] ServerGC: {GCSettings.IsServerGC}, LatencyMode: {GCSettings.LatencyMode}");

            GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

            string ingressSock = args.Length > 0 ? args[0] : "/tmp/engine_ingress.sock";
            string egressSock = args.Length > 1 ? args[1] : "/tmp/engine_egress.sock";

            using var pool = new OrderMemoryPool();
            using var ringBuffer = new RingBuffer(131072);
            using var egressWriter = new EgressWriter(egressSock);
            Console.WriteLine($"[Engine.Core] Connecting to Egress UDS at {egressSock}...");

            _ = Task.Run(() =>
            {
                while (!egressWriter.Connect())
                {
                    Thread.Sleep(200);
                }
                Console.WriteLine($"[Engine.Core] Connected to Egress UDS: {egressSock}");
            });

            using var orderBook = new OrderBook(1, pool, egressWriter);
            using var ingressServer = new UdsServer(ingressSock, ringBuffer);
            ingressServer.Start();
            Console.WriteLine($"[Engine.Core] Ingress Server listening on {ingressSock}");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            var matchingThread = new Thread(() => RunMatchingLoop(ringBuffer, orderBook, egressWriter, cts.Token))
            {
                Name = "CoreMatchingEngineThread",
                Priority = ThreadPriority.Highest,
                IsBackground = false
            };
            matchingThread.Start();

            long lastOrders = 0;
            long lastTrades = 0;

            while (!cts.IsCancellationRequested)
            {
                await Task.Delay(1000, cts.Token).ConfigureAwait(false);
                long currentOrders = (long)orderBook.TotalOrdersProcessed;
                long currentTrades = (long)orderBook.TotalTradesExecuted;
                long dOrders = currentOrders - lastOrders;
                long dTrades = currentTrades - lastTrades;
                lastOrders = currentOrders;
                lastTrades = currentTrades;

                long gen0 = GC.CollectionCount(0);
                long gen1 = GC.CollectionCount(1);
                long gen2 = GC.CollectionCount(2);

                Console.WriteLine($"[Engine.Core] Throughput: {dOrders:N0} orders/s, {dTrades:N0} trades/s | Total: {currentOrders:N0} orders | BestBid: {orderBook.BestBidPrice/10000.0:F4} BestAsk: {orderBook.BestAskPrice/10000.0:F4} | GC (Gen0/1/2): {gen0}/{gen1}/{gen2}");
            }

            Console.WriteLine("[Engine.Core] Shutting down matching engine...");
            matchingThread.Join();
            Console.WriteLine("[Engine.Core] Daemon exited cleanly.");
        }

        private static void RunMatchingLoop(RingBuffer ringBuffer, OrderBook orderBook, EgressWriter egressWriter, CancellationToken ct)
        {
            Console.WriteLine("[MatchingLoop] Deterministic single-threaded matching loop pinned and started.");

            InboundFrame frame;
            int idleSpins = 0;

            while (!ct.IsCancellationRequested)
            {
                if (ringBuffer.TryDequeue(out frame))
                {
                    orderBook.ProcessInboundFrame(in frame);
                    idleSpins = 0;
                }
                else
                {
                    idleSpins++;
                    if (idleSpins < 100)
                    {
                        Thread.SpinWait(20);
                    }
                    else if (idleSpins < 1000)
                    {
                        egressWriter.Flush();
                        Thread.SpinWait(100);
                    }
                    else
                    {
                        egressWriter.Flush();
                        Thread.Yield();
                    }
                }
            }

            egressWriter.Flush();
        }
    }
}
