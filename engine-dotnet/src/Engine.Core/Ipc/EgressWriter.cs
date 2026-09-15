using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using HfEngine.Matching;
using HfEngine.Protocol;

namespace HfEngine.Ipc
{
    public sealed unsafe class EgressWriter : IEgressSink, IDisposable
    {
        private readonly string _socketPath;
        private Socket? _socket;
        private readonly byte* _sendBuffer;
        private const int BufferSize = 65536;
        private int _bufferOffset;
        private bool _disposed;

        public ulong TradesEmitted { get; private set; }
        public ulong AcksEmitted { get; private set; }
        public ulong DeltasEmitted { get; private set; }

        public EgressWriter(string socketPath = "/tmp/engine_egress.sock")
        {
            _socketPath = socketPath;
            _sendBuffer = (byte*)NativeMemory.Alloc((nuint)BufferSize);
            _bufferOffset = 0;
        }

        public bool Connect()
        {
            try
            {
                var endPoint = new UnixDomainSocketEndPoint(_socketPath);
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                socket.SendBufferSize = 4 * 1024 * 1024;
                socket.Connect(endPoint);
                _socket = socket;
                return true;
            }
            catch
            {
                _socket = null;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EmitAck(in OrderAckPayload ack)
        {
            AcksEmitted++;
            EnsureBufferSpace(ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderAckPayloadSize);

            ref byte dst = ref _sendBuffer[_bufferOffset];
            FrameHeader header = new FrameHeader(ProtocolConstants.MsgTypeOrderAck, 0, ProtocolConstants.OrderAckPayloadSize);
            Unsafe.WriteUnaligned(ref dst, header);

            ref byte payloadDst = ref Unsafe.Add(ref dst, ProtocolConstants.FrameHeaderSize);
            Unsafe.WriteUnaligned(ref payloadDst, ack);

            _bufferOffset += ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderAckPayloadSize;
            FlushIfNeeded();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EmitTrade(in OrderExecutedPayload trade)
        {
            TradesEmitted++;
            EnsureBufferSpace(ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderExecutedSize);

            ref byte dst = ref _sendBuffer[_bufferOffset];
            FrameHeader header = new FrameHeader(ProtocolConstants.MsgTypeOrderExecuted, 0, ProtocolConstants.OrderExecutedSize);
            Unsafe.WriteUnaligned(ref dst, header);

            ref byte payloadDst = ref Unsafe.Add(ref dst, ProtocolConstants.FrameHeaderSize);
            Unsafe.WriteUnaligned(ref payloadDst, trade);

            _bufferOffset += ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderExecutedSize;
            FlushIfNeeded();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EmitL2Delta(in OrderBookL2DeltaPayload delta)
        {
            DeltasEmitted++;
            EnsureBufferSpace(ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderBookL2DeltaSize);

            ref byte dst = ref _sendBuffer[_bufferOffset];
            FrameHeader header = new FrameHeader(ProtocolConstants.MsgTypeOrderBookL2Delta, 0, ProtocolConstants.OrderBookL2DeltaSize);
            Unsafe.WriteUnaligned(ref dst, header);

            ref byte payloadDst = ref Unsafe.Add(ref dst, ProtocolConstants.FrameHeaderSize);
            Unsafe.WriteUnaligned(ref payloadDst, delta);

            _bufferOffset += ProtocolConstants.FrameHeaderSize + ProtocolConstants.OrderBookL2DeltaSize;
            FlushIfNeeded();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBufferSpace(int bytesNeeded)
        {
            if (_bufferOffset + bytesNeeded > BufferSize)
            {
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FlushIfNeeded()
        {
            if (_bufferOffset >= 4096)
            {
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            if (_bufferOffset == 0 || _socket == null) return;

            try
            {
                ReadOnlySpan<byte> toSend = new ReadOnlySpan<byte>(_sendBuffer, _bufferOffset);
                _socket.Send(toSend, SocketFlags.None);
            }
            catch
            {
            }
            finally
            {
                _bufferOffset = 0;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Flush();
                _socket?.Dispose();
                NativeMemory.Free(_sendBuffer);
                _disposed = true;
            }
        }
    }
}
