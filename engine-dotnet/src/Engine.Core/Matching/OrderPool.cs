using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HfEngine.Matching
{
    [StructLayout(LayoutKind.Sequential)]
    public struct OrderRecord
    {
        public ulong OrderId;
        public ulong TraderId;
        public long Price;
        public uint RemainingQty;
        public uint InitialQty;
        public byte Side;
        public byte Flags;
        public int PriceLevelIndex;
        public int NextOrderIndex;
        public int PrevOrderIndex;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct PriceLevelRecord
    {
        public long Price;
        public uint TotalQuantity;
        public int OrderCount;
        public int HeadOrderIndex;
        public int TailOrderIndex;
        public int NextLevelIndex;
        public int PrevLevelIndex;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct OrderIndexEntry
    {
        public ulong OrderId;
        public int NodeIndex;
    }

    public sealed unsafe class OrderMemoryPool : IDisposable
    {
        public const int MaxOrders = 262144;
        public const int MaxLevels = 65536;
        public const int IndexCapacity = 524288;
        private const int IndexMask = IndexCapacity - 1;

        public readonly OrderRecord* Orders;
        public readonly PriceLevelRecord* Levels;
        public readonly OrderIndexEntry* IndexTable;

        private readonly int* _freeOrders;
        private int _freeOrderTop;

        private readonly int* _freeLevels;
        private int _freeLevelTop;

        private bool _disposed;

        public OrderMemoryPool()
        {
            Orders = (OrderRecord*)NativeMemory.AllocZeroed((nuint)(MaxOrders * sizeof(OrderRecord)));
            Levels = (PriceLevelRecord*)NativeMemory.AllocZeroed((nuint)(MaxLevels * sizeof(PriceLevelRecord)));
            IndexTable = (OrderIndexEntry*)NativeMemory.AllocZeroed((nuint)(IndexCapacity * sizeof(OrderIndexEntry)));

            _freeOrders = (int*)NativeMemory.Alloc((nuint)(MaxOrders * sizeof(int)));
            _freeOrderTop = MaxOrders;
            for (int i = 0; i < MaxOrders; i++)
            {
                _freeOrders[i] = MaxOrders - 1 - i;
            }

            _freeLevels = (int*)NativeMemory.Alloc((nuint)(MaxLevels * sizeof(int)));
            _freeLevelTop = MaxLevels;
            for (int i = 0; i < MaxLevels; i++)
            {
                _freeLevels[i] = MaxLevels - 1 - i;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AllocateOrder()
        {
            if (_freeOrderTop <= 0)
                return -1;
            return _freeOrders[--_freeOrderTop];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseOrder(int index)
        {
            Orders[index] = default;
            _freeOrders[_freeOrderTop++] = index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AllocateLevel()
        {
            if (_freeLevelTop <= 0)
                return -1;
            int idx = _freeLevels[--_freeLevelTop];
            Levels[idx] = default;
            Levels[idx].HeadOrderIndex = -1;
            Levels[idx].TailOrderIndex = -1;
            Levels[idx].NextLevelIndex = -1;
            Levels[idx].PrevLevelIndex = -1;
            return idx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseLevel(int index)
        {
            Levels[index] = default;
            _freeLevels[_freeLevelTop++] = index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PutIndex(ulong orderId, int nodeIndex)
        {
            ulong hash = orderId * 11400714819323198485UL;
            int idx = (int)(hash & IndexMask);

            while (true)
            {
                if (IndexTable[idx].OrderId == 0 || IndexTable[idx].OrderId == orderId)
                {
                    IndexTable[idx].OrderId = orderId;
                    IndexTable[idx].NodeIndex = nodeIndex;
                    return;
                }
                idx = (idx + 1) & IndexMask;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int FindIndex(ulong orderId)
        {
            ulong hash = orderId * 11400714819323198485UL;
            int idx = (int)(hash & IndexMask);

            while (true)
            {
                ulong current = IndexTable[idx].OrderId;
                if (current == orderId)
                {
                    return IndexTable[idx].NodeIndex;
                }
                if (current == 0)
                {
                    return -1;
                }
                idx = (idx + 1) & IndexMask;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RemoveIndex(ulong orderId)
        {
            ulong hash = orderId * 11400714819323198485UL;
            int idx = (int)(hash & IndexMask);

            while (true)
            {
                ulong current = IndexTable[idx].OrderId;
                if (current == orderId)
                {
                    IndexTable[idx].OrderId = 0;
                    IndexTable[idx].NodeIndex = -1;
                    return;
                }
                if (current == 0)
                {
                    return;
                }
                idx = (idx + 1) & IndexMask;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                NativeMemory.Free(Orders);
                NativeMemory.Free(Levels);
                NativeMemory.Free(IndexTable);
                NativeMemory.Free(_freeOrders);
                NativeMemory.Free(_freeLevels);
                _disposed = true;
            }
        }
    }
}
