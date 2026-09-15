using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using HfEngine.Disruptor;
using HfEngine.Protocol;

namespace HfEngine.Ipc
{
    public sealed class UdsServer : IDisposable
    {
        private readonly string _socketPath;
        private readonly RingBuffer _ringBuffer;
        private Socket? _listenerSocket;
        private CancellationTokenSource? _cts;
        private bool _disposed;

        public ulong TotalFramesEnqueued { get; private set; }

        public UdsServer(string socketPath, RingBuffer ringBuffer)
        {
            _socketPath = socketPath;
            _ringBuffer = ringBuffer;
        }

        public void Start()
        {
            if (File.Exists(_socketPath))
            {
                try { File.Delete(_socketPath); } catch { }
            }

            _cts = new CancellationTokenSource();
            var endPoint = new UnixDomainSocketEndPoint(_socketPath);
            _listenerSocket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            _listenerSocket.ReceiveBufferSize = 4 * 1024 * 1024;
            _listenerSocket.Bind(endPoint);
            _listenerSocket.Listen(128);

            Task.Run(() => AcceptLoopAsync(_cts.Token));
        }

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && _listenerSocket != null)
            {
                try
                {
                    Socket clientSocket = await _listenerSocket.AcceptAsync(ct).ConfigureAwait(false);
                    _ = Task.Run(() => ProcessClientAsync(clientSocket, ct), ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    if (ct.IsCancellationRequested) break;
                }
            }
        }

        private async Task ProcessClientAsync(Socket clientSocket, CancellationToken ct)
        {
            using (clientSocket)
            using (var stream = new NetworkStream(clientSocket, ownsSocket: false))
            {
                var pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(
                    pool: MemoryPool<byte>.Shared,
                    bufferSize: 65536,
                    minimumReadSize: 1024));

                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        ReadResult result = await pipeReader.ReadAsync(ct).ConfigureAwait(false);
                        ReadOnlySequence<byte> buffer = result.Buffer;

                        while (FrameParser.TryReadFrame(ref buffer, out InboundFrame frame))
                        {
                            while (!_ringBuffer.TryEnqueue(in frame))
                            {
                                Thread.SpinWait(10);
                            }
                            TotalFramesEnqueued++;
                        }

                        pipeReader.AdvanceTo(buffer.Start, buffer.End);

                        if (result.IsCompleted || result.IsCanceled)
                        {
                            break;
                        }
                    }
                }
                catch (Exception)
                {
                }
                finally
                {
                    await pipeReader.CompleteAsync().ConfigureAwait(false);
                }
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _cts?.Cancel();
                _listenerSocket?.Dispose();
                if (File.Exists(_socketPath))
                {
                    try { File.Delete(_socketPath); } catch { }
                }
                _cts?.Dispose();
                _disposed = true;
            }
        }
    }
}
