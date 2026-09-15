using System;
using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using HfEngine.Protocol;

namespace HfEngine.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net90)]
    public class ParserBenchmarks
    {
        private byte[] _rawBytes = null!;
        private ReadOnlySequence<byte> _sequence;

        [GlobalSetup]
        public void Setup()
        {
            int frameCount = 1000;
            _rawBytes = new byte[frameCount * 40];
            Span<byte> span = _rawBytes.AsSpan();

            for (int i = 0; i < frameCount; i++)
            {
                var header = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, 0, 32);
                var payload = new NewOrderPayload((ulong)(i + 1), 100, 1000000, 10, 1);
                int offset = i * 40;

                System.Runtime.CompilerServices.Unsafe.WriteUnaligned(ref span[offset], header);
                System.Runtime.CompilerServices.Unsafe.WriteUnaligned(ref span[offset + 8], payload);
            }

            _sequence = new ReadOnlySequence<byte>(_rawBytes);
        }

        [Benchmark]
        public int ParseFrames_ZeroAlloc()
        {
            var seq = _sequence;
            int count = 0;
            while (FrameParser.TryReadFrame(ref seq, out InboundFrame frame))
            {
                count++;
            }
            return count;
        }
    }
}
