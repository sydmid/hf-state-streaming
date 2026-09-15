using System;
using System.Runtime.CompilerServices;
using HfEngine.Protocol;

namespace HfEngine.Matching
{
    public interface IEgressSink
    {
        void EmitAck(in OrderAckPayload ack);
        void EmitTrade(in OrderExecutedPayload trade);
        void EmitL2Delta(in OrderBookL2DeltaPayload delta);
    }

    public sealed unsafe class OrderBook : IDisposable
    {
        public readonly uint AssetId;
        private readonly OrderMemoryPool _pool;
        private readonly IEgressSink _sink;

        private int _bestBidLevelIndex = -1;
        private int _bestAskLevelIndex = -1;

        private ulong _nextTradeId = 1;
        private ulong _totalOrdersProcessed = 0;
        private ulong _totalTradesExecuted = 0;

        public OrderBook(uint assetId, OrderMemoryPool pool, IEgressSink sink)
        {
            AssetId = assetId;
            _pool = pool;
            _sink = sink;
        }

        public long BestBidPrice => _bestBidLevelIndex != -1 ? _pool.Levels[_bestBidLevelIndex].Price : 0;
        public long BestAskPrice => _bestAskLevelIndex != -1 ? _pool.Levels[_bestAskLevelIndex].Price : 0;
        public ulong TotalOrdersProcessed => _totalOrdersProcessed;
        public ulong TotalTradesExecuted => _totalTradesExecuted;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProcessInboundFrame(in InboundFrame frame)
        {
            _totalOrdersProcessed++;
            if (frame.Header.MsgType == ProtocolConstants.MsgTypeNewOrder)
            {
                ProcessNewOrder(in frame);
            }
            else if (frame.Header.MsgType == ProtocolConstants.MsgTypeCancelOrder)
            {
                ProcessCancelOrder(in frame);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessNewOrder(in InboundFrame frame)
        {
            bool isBuy = frame.Header.IsBuy;
            bool isIOC = frame.Header.IsIOC;
            bool isPostOnly = frame.Header.IsPostOnly;

            long price = frame.Price;
            uint remainingQty = frame.Quantity;
            ulong orderId = frame.OrderId;
            ulong traderId = frame.PlayerOrTraderId;

            if (isPostOnly)
            {
                bool wouldCross = isBuy
                    ? (_bestAskLevelIndex != -1 && price >= _pool.Levels[_bestAskLevelIndex].Price)
                    : (_bestBidLevelIndex != -1 && price <= _pool.Levels[_bestBidLevelIndex].Price);

                if (wouldCross)
                {
                    _sink.EmitAck(new OrderAckPayload(orderId, traderId, ProtocolConstants.AckStatusRejected));
                    return;
                }
            }

            if (isBuy)
            {
                while (remainingQty > 0 && _bestAskLevelIndex != -1)
                {
                    PriceLevelRecord* askLevel = &_pool.Levels[_bestAskLevelIndex];
                    if (price < askLevel->Price)
                        break;

                    int makerNodeIdx = askLevel->HeadOrderIndex;
                    while (remainingQty > 0 && makerNodeIdx != -1)
                    {
                        OrderRecord* maker = &_pool.Orders[makerNodeIdx];
                        int nextMakerIdx = maker->NextOrderIndex;

                        uint execQty = Math.Min(remainingQty, maker->RemainingQty);
                        maker->RemainingQty -= execQty;
                        remainingQty -= execQty;
                        askLevel->TotalQuantity -= execQty;

                        _sink.EmitTrade(new OrderExecutedPayload(_nextTradeId++, maker->OrderId, orderId, maker->Price));
                        _totalTradesExecuted++;

                        if (maker->RemainingQty == 0)
                        {
                            RemoveOrderFromLevel(askLevel, makerNodeIdx);
                            _pool.RemoveIndex(maker->OrderId);
                            _pool.ReleaseOrder(makerNodeIdx);
                        }

                        makerNodeIdx = nextMakerIdx;
                    }

                    _sink.EmitL2Delta(new OrderBookL2DeltaPayload(AssetId, askLevel->Price, askLevel->TotalQuantity, ProtocolConstants.FlagSideSell));

                    if (askLevel->OrderCount == 0)
                    {
                        int nextLevel = askLevel->NextLevelIndex;
                        RemovePriceLevel(ref _bestAskLevelIndex, _bestAskLevelIndex);
                        _bestAskLevelIndex = nextLevel;
                    }
                }
            }
            else
            {
                while (remainingQty > 0 && _bestBidLevelIndex != -1)
                {
                    PriceLevelRecord* bidLevel = &_pool.Levels[_bestBidLevelIndex];
                    if (price > bidLevel->Price)
                        break;

                    int makerNodeIdx = bidLevel->HeadOrderIndex;
                    while (remainingQty > 0 && makerNodeIdx != -1)
                    {
                        OrderRecord* maker = &_pool.Orders[makerNodeIdx];
                        int nextMakerIdx = maker->NextOrderIndex;

                        uint execQty = Math.Min(remainingQty, maker->RemainingQty);
                        maker->RemainingQty -= execQty;
                        remainingQty -= execQty;
                        bidLevel->TotalQuantity -= execQty;

                        _sink.EmitTrade(new OrderExecutedPayload(_nextTradeId++, maker->OrderId, orderId, maker->Price));
                        _totalTradesExecuted++;

                        if (maker->RemainingQty == 0)
                        {
                            RemoveOrderFromLevel(bidLevel, makerNodeIdx);
                            _pool.RemoveIndex(maker->OrderId);
                            _pool.ReleaseOrder(makerNodeIdx);
                        }

                        makerNodeIdx = nextMakerIdx;
                    }

                    _sink.EmitL2Delta(new OrderBookL2DeltaPayload(AssetId, bidLevel->Price, bidLevel->TotalQuantity, ProtocolConstants.FlagSideBuy));

                    if (bidLevel->OrderCount == 0)
                    {
                        int nextLevel = bidLevel->NextLevelIndex;
                        RemovePriceLevel(ref _bestBidLevelIndex, _bestBidLevelIndex);
                        _bestBidLevelIndex = nextLevel;
                    }
                }
            }

            if (remainingQty > 0)
            {
                if (isIOC)
                {
                    _sink.EmitAck(new OrderAckPayload(orderId, traderId, ProtocolConstants.AckStatusCanceled));
                }
                else
                {
                    int orderIdx = _pool.AllocateOrder();
                    if (orderIdx == -1)
                    {
                        _sink.EmitAck(new OrderAckPayload(orderId, traderId, ProtocolConstants.AckStatusRejected));
                        return;
                    }

                    OrderRecord* resting = &_pool.Orders[orderIdx];
                    resting->OrderId = orderId;
                    resting->TraderId = traderId;
                    resting->Price = price;
                    resting->RemainingQty = remainingQty;
                    resting->InitialQty = frame.Quantity;
                    resting->Side = isBuy ? ProtocolConstants.FlagSideBuy : ProtocolConstants.FlagSideSell;
                    resting->Flags = frame.Header.Flags;
                    resting->NextOrderIndex = -1;
                    resting->PrevOrderIndex = -1;

                    _pool.PutIndex(orderId, orderIdx);

                    int levelIdx = GetOrCreateLevel(price, isBuy);
                    PriceLevelRecord* level = &_pool.Levels[levelIdx];
                    resting->PriceLevelIndex = levelIdx;

                    AppendOrderToLevel(level, orderIdx);
                    level->TotalQuantity += remainingQty;

                    _sink.EmitAck(new OrderAckPayload(orderId, traderId, ProtocolConstants.AckStatusAccepted));
                    _sink.EmitL2Delta(new OrderBookL2DeltaPayload(AssetId, price, level->TotalQuantity, isBuy ? ProtocolConstants.FlagSideBuy : ProtocolConstants.FlagSideSell));
                }
            }
            else
            {
                _sink.EmitAck(new OrderAckPayload(orderId, traderId, ProtocolConstants.AckStatusAccepted));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessCancelOrder(in InboundFrame frame)
        {
            ulong orderId = frame.OrderId;
            int orderIdx = _pool.FindIndex(orderId);
            if (orderIdx == -1)
            {
                _sink.EmitAck(new OrderAckPayload(orderId, frame.PlayerOrTraderId, ProtocolConstants.AckStatusRejected));
                return;
            }

            OrderRecord* order = &_pool.Orders[orderIdx];
            int levelIdx = order->PriceLevelIndex;
            PriceLevelRecord* level = &_pool.Levels[levelIdx];

            level->TotalQuantity -= order->RemainingQty;
            RemoveOrderFromLevel(level, orderIdx);

            _sink.EmitAck(new OrderAckPayload(orderId, frame.PlayerOrTraderId, ProtocolConstants.AckStatusCanceled));
            _sink.EmitL2Delta(new OrderBookL2DeltaPayload(AssetId, level->Price, level->TotalQuantity, order->Side));

            if (level->OrderCount == 0)
            {
                if (order->Side == ProtocolConstants.FlagSideBuy)
                {
                    RemovePriceLevel(ref _bestBidLevelIndex, levelIdx);
                }
                else
                {
                    RemovePriceLevel(ref _bestAskLevelIndex, levelIdx);
                }
            }

            _pool.RemoveIndex(orderId);
            _pool.ReleaseOrder(orderIdx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetOrCreateLevel(long price, bool isBuy)
        {
            int current = isBuy ? _bestBidLevelIndex : _bestAskLevelIndex;
            int prev = -1;

            while (current != -1)
            {
                PriceLevelRecord* lvl = &_pool.Levels[current];
                if (lvl->Price == price)
                    return current;

                if (isBuy ? (price > lvl->Price) : (price < lvl->Price))
                {
                    int newLvlIdx = _pool.AllocateLevel();
                    PriceLevelRecord* newLvl = &_pool.Levels[newLvlIdx];
                    newLvl->Price = price;
                    newLvl->NextLevelIndex = current;
                    newLvl->PrevLevelIndex = prev;

                    lvl->PrevLevelIndex = newLvlIdx;
                    if (prev != -1)
                        _pool.Levels[prev].NextLevelIndex = newLvlIdx;
                    else
                    {
                        if (isBuy) _bestBidLevelIndex = newLvlIdx;
                        else _bestAskLevelIndex = newLvlIdx;
                    }
                    return newLvlIdx;
                }

                prev = current;
                current = lvl->NextLevelIndex;
            }

            int tailIdx = _pool.AllocateLevel();
            PriceLevelRecord* tailLvl = &_pool.Levels[tailIdx];
            tailLvl->Price = price;
            tailLvl->NextLevelIndex = -1;
            tailLvl->PrevLevelIndex = prev;

            if (prev != -1)
            {
                _pool.Levels[prev].NextLevelIndex = tailIdx;
            }
            else
            {
                if (isBuy) _bestBidLevelIndex = tailIdx;
                else _bestAskLevelIndex = tailIdx;
            }
            return tailIdx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AppendOrderToLevel(PriceLevelRecord* level, int orderIdx)
        {
            OrderRecord* order = &_pool.Orders[orderIdx];
            order->NextOrderIndex = -1;
            order->PrevOrderIndex = level->TailOrderIndex;

            if (level->TailOrderIndex != -1)
            {
                _pool.Orders[level->TailOrderIndex].NextOrderIndex = orderIdx;
            }
            else
            {
                level->HeadOrderIndex = orderIdx;
            }
            level->TailOrderIndex = orderIdx;
            level->OrderCount++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RemoveOrderFromLevel(PriceLevelRecord* level, int orderIdx)
        {
            OrderRecord* order = &_pool.Orders[orderIdx];
            int prev = order->PrevOrderIndex;
            int next = order->NextOrderIndex;

            if (prev != -1)
                _pool.Orders[prev].NextOrderIndex = next;
            else
                level->HeadOrderIndex = next;

            if (next != -1)
                _pool.Orders[next].PrevOrderIndex = prev;
            else
                level->TailOrderIndex = prev;

            level->OrderCount--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RemovePriceLevel(ref int rootIndex, int levelIdx)
        {
            PriceLevelRecord* lvl = &_pool.Levels[levelIdx];
            int prev = lvl->PrevLevelIndex;
            int next = lvl->NextLevelIndex;

            if (prev != -1)
                _pool.Levels[prev].NextLevelIndex = next;
            else
                rootIndex = next;

            if (next != -1)
                _pool.Levels[next].PrevLevelIndex = prev;

            _pool.ReleaseLevel(levelIdx);
        }

        public void Dispose()
        {
        }
    }
}
