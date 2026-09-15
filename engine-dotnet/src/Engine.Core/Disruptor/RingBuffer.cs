using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using HfEngine.Protocol;

namespace HfEngine.Disruptor
{
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    public struct PaddedSequence
    {
        [FieldOffset(24)]
        public long Value;

        public PaddedSequence(long initialValue)
        {
            Value = initialValue;
        }
    }

    public sealed unsafe class RingBuffer : IDisposable
    {
        private readonly int _capacity;
        private readonly int _mask;
        private readonly InboundFrame* _buffer;
        private bool _disposed;

        private PaddedSequence _producerSequence;
        private PaddedSequence _consumerSequence;

        public RingBuffer(int capacity = 131072)
        {
            if ((capacity & (capacity - 1)) != 0)
                throw new ArgumentException("Capacity must be a power of two", nameof(capacity));

            _capacity = capacity;
            _mask = capacity - 1;

            long byteSize = (long)capacity * sizeof(InboundFrame);
            _buffer = (InboundFrame*)NativeMemory.AllocZeroed((nuint)byteSize);

            _producerSequence = new PaddedSequence(0);
            _consumerSequence = new PaddedSequence(0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(in InboundFrame item)
        {
            long currentProducer = Volatile.Read(ref _producerSequence.Value);
            long currentConsumer = Volatile.Read(ref _consumerSequence.Value);

            if (currentProducer - currentConsumer >= _capacity)
            {
                return false;
            }

            int index = (int)(currentProducer & _mask);
            _buffer[index] = item;

            Volatile.Write(ref _producerSequence.Value, currentProducer + 1);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out InboundFrame item)
        {
            long currentConsumer = Volatile.Read(ref _consumerSequence.Value);
            long currentProducer = Volatile.Read(ref _producerSequence.Value);

            if (currentConsumer >= currentProducer)
            {
                Unsafe.SkipInit(out item);
                return false;
            }

            int index = (int)(currentConsumer & _mask);
            item = _buffer[index];

            Volatile.Write(ref _consumerSequence.Value, currentConsumer + 1);
            return true;
        }

        public int Count
        {
            get
            {
                long p = Volatile.Read(ref _producerSequence.Value);
                long c = Volatile.Read(ref _consumerSequence.Value);
                return (int)(p - c);
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                NativeMemory.Free(_buffer);
                _disposed = true;
            }
        }
    }
}
