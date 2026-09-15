using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HfEngine.Protocol
{
    public static class ProtocolConstants
    {
        public const ushort FrameMagic = 0x534D;

        public const byte MsgTypeNewOrder = 0x01;
        public const byte MsgTypeCancelOrder = 0x02;
        public const byte MsgTypeOrderAck = 0x03;
        public const byte MsgTypeOrderExecuted = 0x04;
        public const byte MsgTypeOrderBookL2Delta = 0x05;

        public const byte FlagSideBuy = 0x00;
        public const byte FlagSideSell = 0x01;
        public const byte FlagIOC = 0x02;
        public const byte FlagPostOnly = 0x04;

        public const byte AckStatusAccepted = 0x00;
        public const byte AckStatusRejected = 0x01;
        public const byte AckStatusCanceled = 0x02;

        public const int FrameHeaderSize = 8;
        public const int NewOrderPayloadSize = 32;
        public const int CancelOrderPayloadSize = 16;
        public const int OrderAckPayloadSize = 24;
        public const int OrderExecutedSize = 32;
        public const int OrderBookL2DeltaSize = 24;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct FrameHeader
    {
        public readonly ushort Magic;
        public readonly byte MsgType;
        public readonly byte Flags;
        public readonly ushort PayloadLen;
        public readonly ushort Reserved;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FrameHeader(byte msgType, byte flags, ushort payloadLen)
        {
            Magic = ProtocolConstants.FrameMagic;
            MsgType = msgType;
            Flags = flags;
            PayloadLen = payloadLen;
            Reserved = 0;
        }

        public bool IsValid => Magic == ProtocolConstants.FrameMagic;
        public bool IsBuy => (Flags & ProtocolConstants.FlagSideSell) == 0;
        public bool IsSell => (Flags & ProtocolConstants.FlagSideSell) != 0;
        public bool IsIOC => (Flags & ProtocolConstants.FlagIOC) != 0;
        public bool IsPostOnly => (Flags & ProtocolConstants.FlagPostOnly) != 0;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct NewOrderPayload
    {
        public readonly ulong OrderId;
        public readonly ulong PlayerOrTraderId;
        public readonly long Price;
        public readonly uint Quantity;
        public readonly uint AssetId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NewOrderPayload(ulong orderId, ulong playerOrTraderId, long price, uint quantity, uint assetId)
        {
            OrderId = orderId;
            PlayerOrTraderId = playerOrTraderId;
            Price = price;
            Quantity = quantity;
            AssetId = assetId;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct CancelOrderPayload
    {
        public readonly ulong OrderId;
        public readonly ulong PlayerOrTraderId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CancelOrderPayload(ulong orderId, ulong playerOrTraderId)
        {
            OrderId = orderId;
            PlayerOrTraderId = playerOrTraderId;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public unsafe struct OrderAckPayload
    {
        public ulong OrderId;
        public ulong PlayerOrTraderId;
        public byte Status;
        public fixed byte Reserved[7];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OrderAckPayload(ulong orderId, ulong playerOrTraderId, byte status)
        {
            OrderId = orderId;
            PlayerOrTraderId = playerOrTraderId;
            Status = status;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct OrderExecutedPayload
    {
        public readonly ulong TradeId;
        public readonly ulong MakerOrderId;
        public readonly ulong TakerOrderId;
        public readonly long ExecutionPrice;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OrderExecutedPayload(ulong tradeId, ulong makerOrderId, ulong takerOrderId, long executionPrice)
        {
            TradeId = tradeId;
            MakerOrderId = makerOrderId;
            TakerOrderId = takerOrderId;
            ExecutionPrice = executionPrice;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public unsafe struct OrderBookL2DeltaPayload
    {
        public uint AssetId;
        public uint Reserved1;
        public long Price;
        public uint NewQuantity;
        public byte Side;
        public fixed byte Reserved2[3];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OrderBookL2DeltaPayload(uint assetId, long price, uint newQuantity, byte side)
        {
            AssetId = assetId;
            Reserved1 = 0;
            Price = price;
            NewQuantity = newQuantity;
            Side = side;
        }
    }
}
