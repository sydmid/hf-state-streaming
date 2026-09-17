using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HfEngine.Protocol
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct InboundFrame
    {
        public readonly FrameHeader Header;
        public readonly ulong OrderId;
        public readonly ulong PlayerOrTraderId;
        public readonly long Price;
        public readonly uint Quantity;
        public readonly uint AssetId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public InboundFrame(FrameHeader header, ulong orderId, ulong traderId, long price, uint qty, uint assetId)
        {
            Header = header;
            OrderId = orderId;
            PlayerOrTraderId = traderId;
            Price = price;
            Quantity = qty;
            AssetId = assetId;
        }
    }

    public static class FrameParser
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool TryReadFrame(ref SequenceReader<byte> reader, out InboundFrame frame)
        {
            if (reader.Remaining < ProtocolConstants.FrameHeaderSize)
            {
                Unsafe.SkipInit(out frame);
                return false;
            }

            ReadOnlySpan<byte> unread = reader.UnreadSpan;
            FrameHeader header;
            if (unread.Length >= ProtocolConstants.FrameHeaderSize)
            {
                header = Unsafe.ReadUnaligned<FrameHeader>(ref MemoryMarshal.GetReference(unread));
            }
            else
            {
                Span<byte> headerBytes = stackalloc byte[ProtocolConstants.FrameHeaderSize];
                reader.TryCopyTo(headerBytes);
                header = Unsafe.ReadUnaligned<FrameHeader>(ref MemoryMarshal.GetReference(headerBytes));
            }

            if (!header.IsValid)
            {
                reader.Advance(1);
                Unsafe.SkipInit(out frame);
                return false;
            }

            int totalFrameLength = ProtocolConstants.FrameHeaderSize + header.PayloadLen;
            if (reader.Remaining < totalFrameLength)
            {
                Unsafe.SkipInit(out frame);
                return false;
            }

            reader.Advance(ProtocolConstants.FrameHeaderSize);

            if (header.MsgType == ProtocolConstants.MsgTypeNewOrder)
            {
                NewOrderPayload payload;
                if (reader.UnreadSpan.Length >= ProtocolConstants.NewOrderPayloadSize)
                {
                    payload = Unsafe.ReadUnaligned<NewOrderPayload>(ref MemoryMarshal.GetReference(reader.UnreadSpan));
                    reader.Advance(ProtocolConstants.NewOrderPayloadSize);
                }
                else
                {
                    Span<byte> payloadBytes = stackalloc byte[ProtocolConstants.NewOrderPayloadSize];
                    reader.TryCopyTo(payloadBytes);
                    payload = Unsafe.ReadUnaligned<NewOrderPayload>(ref MemoryMarshal.GetReference(payloadBytes));
                    reader.Advance(ProtocolConstants.NewOrderPayloadSize);
                }
                frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, payload.Price, payload.Quantity, payload.AssetId);
                return true;
            }
            else if (header.MsgType == ProtocolConstants.MsgTypeCancelOrder)
            {
                CancelOrderPayload payload;
                if (reader.UnreadSpan.Length >= ProtocolConstants.CancelOrderPayloadSize)
                {
                    payload = Unsafe.ReadUnaligned<CancelOrderPayload>(ref MemoryMarshal.GetReference(reader.UnreadSpan));
                    reader.Advance(ProtocolConstants.CancelOrderPayloadSize);
                }
                else
                {
                    Span<byte> payloadBytes = stackalloc byte[ProtocolConstants.CancelOrderPayloadSize];
                    reader.TryCopyTo(payloadBytes);
                    payload = Unsafe.ReadUnaligned<CancelOrderPayload>(ref MemoryMarshal.GetReference(payloadBytes));
                    reader.Advance(ProtocolConstants.CancelOrderPayloadSize);
                }
                frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, 0, 0, 0);
                return true;
            }
            else
            {
                Unsafe.SkipInit(out frame);
                reader.Advance(header.PayloadLen);
                return false;
            }
        }
    }
}
