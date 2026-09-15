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
        public static unsafe bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out InboundFrame frame)
        {
            if (buffer.Length < ProtocolConstants.FrameHeaderSize)
            {
                Unsafe.SkipInit(out frame);
                return false;
            }

            FrameHeader header;
            if (buffer.FirstSpan.Length >= ProtocolConstants.FrameHeaderSize)
            {
                header = Unsafe.ReadUnaligned<FrameHeader>(ref MemoryMarshal.GetReference(buffer.FirstSpan));
            }
            else
            {
                Span<byte> headerBytes = stackalloc byte[ProtocolConstants.FrameHeaderSize];
                buffer.Slice(0, ProtocolConstants.FrameHeaderSize).CopyTo(headerBytes);
                header = Unsafe.ReadUnaligned<FrameHeader>(ref MemoryMarshal.GetReference(headerBytes));
            }

            if (!header.IsValid)
            {
                buffer = buffer.Slice(1);
                Unsafe.SkipInit(out frame);
                return false;
            }

            int totalFrameLength = ProtocolConstants.FrameHeaderSize + header.PayloadLen;
            if (buffer.Length < totalFrameLength)
            {
                Unsafe.SkipInit(out frame);
                return false;
            }

            if (header.MsgType == ProtocolConstants.MsgTypeNewOrder)
            {
                if (buffer.FirstSpan.Length >= totalFrameLength)
                {
                    ref byte baseRef = ref MemoryMarshal.GetReference(buffer.FirstSpan);
                    ref byte payloadRef = ref Unsafe.Add(ref baseRef, ProtocolConstants.FrameHeaderSize);
                    NewOrderPayload payload = Unsafe.ReadUnaligned<NewOrderPayload>(ref payloadRef);
                    frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, payload.Price, payload.Quantity, payload.AssetId);
                }
                else
                {
                    Span<byte> fullBytes = stackalloc byte[40];
                    buffer.Slice(0, totalFrameLength).CopyTo(fullBytes);
                    ref byte payloadRef = ref fullBytes[ProtocolConstants.FrameHeaderSize];
                    NewOrderPayload payload = Unsafe.ReadUnaligned<NewOrderPayload>(ref payloadRef);
                    frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, payload.Price, payload.Quantity, payload.AssetId);
                }
            }
            else if (header.MsgType == ProtocolConstants.MsgTypeCancelOrder)
            {
                if (buffer.FirstSpan.Length >= totalFrameLength)
                {
                    ref byte baseRef = ref MemoryMarshal.GetReference(buffer.FirstSpan);
                    ref byte payloadRef = ref Unsafe.Add(ref baseRef, ProtocolConstants.FrameHeaderSize);
                    CancelOrderPayload payload = Unsafe.ReadUnaligned<CancelOrderPayload>(ref payloadRef);
                    frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, 0, 0, 0);
                }
                else
                {
                    Span<byte> fullBytes = stackalloc byte[24];
                    buffer.Slice(0, totalFrameLength).CopyTo(fullBytes);
                    ref byte payloadRef = ref fullBytes[ProtocolConstants.FrameHeaderSize];
                    CancelOrderPayload payload = Unsafe.ReadUnaligned<CancelOrderPayload>(ref payloadRef);
                    frame = new InboundFrame(header, payload.OrderId, payload.PlayerOrTraderId, 0, 0, 0);
                }
            }
            else
            {
                Unsafe.SkipInit(out frame);
                buffer = buffer.Slice(totalFrameLength);
                return false;
            }

            buffer = buffer.Slice(totalFrameLength);
            return true;
        }
    }
}
