using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using HfEngine.Matching;
using HfEngine.Protocol;

namespace HfEngine.Benchmarks
{
    public sealed class ZeroAllocNullSink : IEgressSink
    {
        public void EmitAck(in OrderAckPayload ack) { }
        public void EmitTrade(in OrderExecutedPayload trade) { }
        public void EmitL2Delta(in OrderBookL2DeltaPayload delta) { }
    }

    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net90)]
    public class MatchingBenchmarks
    {
        private OrderMemoryPool _pool = null!;
        private ZeroAllocNullSink _sink = null!;
        private OrderBook _book = null!;
        private InboundFrame[] _testOrders = null!;

        [Params(10000, 100000)]
        public int OrderCount;

        [GlobalSetup]
        public void Setup()
        {
            _pool = new OrderMemoryPool();
            _sink = new ZeroAllocNullSink();
            _book = new OrderBook(1, _pool, _sink);

            _testOrders = new InboundFrame[OrderCount];
            for (int i = 0; i < OrderCount; i++)
            {
                byte side = (i % 2 == 0) ? ProtocolConstants.FlagSideBuy : ProtocolConstants.FlagSideSell;
                long price = 1000000 + ((i % 100) - 50) * 100;
                var header = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, side, 32);
                _testOrders[i] = new InboundFrame(header, (ulong)(i + 1), (ulong)(1000 + i % 20), price, 10, 1);
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _pool.Dispose();
        }

        [Benchmark]
        public void ProcessOrders_ZeroAllocHotPath()
        {
            for (int i = 0; i < _testOrders.Length; i++)
            {
                _book.ProcessInboundFrame(in _testOrders[i]);
            }
        }
    }
}
