using System;
using System.Collections.Generic;
using HfEngine.Matching;
using HfEngine.Protocol;
using Xunit;

namespace HfEngine.Tests
{
    public class MockEgressSink : IEgressSink
    {
        public List<OrderAckPayload> Acks = new();
        public List<OrderExecutedPayload> Trades = new();
        public List<OrderBookL2DeltaPayload> Deltas = new();

        public void EmitAck(in OrderAckPayload ack) => Acks.Add(ack);
        public void EmitTrade(in OrderExecutedPayload trade) => Trades.Add(trade);
        public void EmitL2Delta(in OrderBookL2DeltaPayload delta) => Deltas.Add(delta);

        public void Clear()
        {
            Acks.Clear();
            Trades.Clear();
            Deltas.Clear();
        }
    }

    public class OrderBookTests : IDisposable
    {
        private readonly OrderMemoryPool _pool;
        private readonly MockEgressSink _sink;
        private readonly OrderBook _book;

        public OrderBookTests()
        {
            _pool = new OrderMemoryPool();
            _sink = new MockEgressSink();
            _book = new OrderBook(1, _pool, _sink);
        }

        public void Dispose()
        {
            _pool.Dispose();
        }

        [Fact]
        public void NewOrder_RestingBuyAndSell_EstablishesSpread()
        {
            var buyHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideBuy, 32);
            var buyFrame = new InboundFrame(buyHeader, 1, 100, 1000000, 10, 1);
            _book.ProcessInboundFrame(in buyFrame);

            var sellHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideSell, 32);
            var sellFrame = new InboundFrame(sellHeader, 2, 200, 1020000, 15, 1);
            _book.ProcessInboundFrame(in sellFrame);

            Assert.Equal(1000000, _book.BestBidPrice);
            Assert.Equal(1020000, _book.BestAskPrice);
            Assert.Empty(_sink.Trades);
            Assert.Equal(2, _sink.Acks.Count);
            Assert.Equal(ProtocolConstants.AckStatusAccepted, _sink.Acks[0].Status);
            Assert.Equal(ProtocolConstants.AckStatusAccepted, _sink.Acks[1].Status);
        }

        [Fact]
        public void CrossingOrder_ExecutesTrade_MatchesPriceTimePriority()
        {
            var sellHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideSell, 32);
            var sell1 = new InboundFrame(sellHeader, 10, 100, 1010000, 10, 1);
            _book.ProcessInboundFrame(in sell1);

            var sell2 = new InboundFrame(sellHeader, 11, 101, 1010000, 20, 1);
            _book.ProcessInboundFrame(in sell2);

            _sink.Clear();

            var buyHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideBuy, 32);
            var buyTaker = new InboundFrame(buyHeader, 20, 200, 1010000, 15, 1);
            _book.ProcessInboundFrame(in buyTaker);

            Assert.Equal(2, _sink.Trades.Count);
            Assert.Equal((ulong)10, _sink.Trades[0].MakerOrderId);
            Assert.Equal((ulong)20, _sink.Trades[0].TakerOrderId);

            Assert.Equal((ulong)11, _sink.Trades[1].MakerOrderId);
            Assert.Equal((ulong)20, _sink.Trades[1].TakerOrderId);

            Assert.Equal(1010000, _book.BestAskPrice);
        }

        [Fact]
        public void PostOnlyOrder_RejectsWhenCrossingSpread()
        {
            var buyHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideBuy, 32);
            var buyFrame = new InboundFrame(buyHeader, 1, 100, 1000000, 10, 1);
            _book.ProcessInboundFrame(in buyFrame);

            _sink.Clear();

            byte flags = ProtocolConstants.FlagSideSell | ProtocolConstants.FlagPostOnly;
            var sellHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, flags, 32);
            var sellFrame = new InboundFrame(sellHeader, 2, 200, 990000, 5, 1);
            _book.ProcessInboundFrame(in sellFrame);

            Assert.Single(_sink.Acks);
            Assert.Equal(ProtocolConstants.AckStatusRejected, _sink.Acks[0].Status);
            Assert.Empty(_sink.Trades);
        }

        [Fact]
        public void IOCOrder_CancelsUnfilledPortion()
        {
            var sellHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideSell, 32);
            var sellFrame = new InboundFrame(sellHeader, 1, 100, 1050000, 5, 1);
            _book.ProcessInboundFrame(in sellFrame);

            _sink.Clear();

            byte flags = ProtocolConstants.FlagSideBuy | ProtocolConstants.FlagIOC;
            var buyHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, flags, 32);
            var buyFrame = new InboundFrame(buyHeader, 2, 200, 1050000, 20, 1);
            _book.ProcessInboundFrame(in buyFrame);

            Assert.Single(_sink.Trades);
            Assert.Contains(_sink.Acks, a => a.Status == ProtocolConstants.AckStatusCanceled);
            Assert.Equal(0, _book.BestAskPrice);
            Assert.Equal(0, _book.BestBidPrice);
        }

        [Fact]
        public void CancelOrder_RemovesFromBookAndUpdatesDepth()
        {
            var buyHeader = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, ProtocolConstants.FlagSideBuy, 32);
            var buyFrame = new InboundFrame(buyHeader, 55, 100, 950000, 50, 1);
            _book.ProcessInboundFrame(in buyFrame);
            Assert.Equal(950000, _book.BestBidPrice);

            _sink.Clear();

            var cancelHeader = new FrameHeader(ProtocolConstants.MsgTypeCancelOrder, 0, 16);
            var cancelFrame = new InboundFrame(cancelHeader, 55, 100, 0, 0, 0);
            _book.ProcessInboundFrame(in cancelFrame);

            Assert.Single(_sink.Acks);
            Assert.Equal(ProtocolConstants.AckStatusCanceled, _sink.Acks[0].Status);
            Assert.Equal(0, _book.BestBidPrice);
        }
    }
}
