using System;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using HfEngine.Protocol;

namespace Demo.WebUi
{
    public class BotWorker : BackgroundService
    {
        private readonly ILogger<BotWorker> _logger;
        private readonly Random _random = new Random();
        private ulong _orderIdCounter = 1;

        public BotWorker(ILogger<BotWorker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("BotWorker started. Delaying 3s to let edge server start...");
            await Task.Delay(3000, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                using var ws = new ClientWebSocket();
                try
                {
                    await ws.ConnectAsync(new Uri("ws://127.0.0.1:8080/ws"), stoppingToken);
                    _logger.LogInformation("BotWorker connected to edge server.");

                    while (ws.State == WebSocketState.Open && !stoppingToken.IsCancellationRequested)
                    {
                        var isBuy = _random.Next(2) == 0;
                        var flags = isBuy ? ProtocolConstants.FlagSideBuy : ProtocolConstants.FlagSideSell;

                        var header = new FrameHeader(ProtocolConstants.MsgTypeNewOrder, flags, (ushort)ProtocolConstants.NewOrderPayloadSize);

                        // Generate random price centered around 100.0000
                        long priceBase = 1000000; // 100.0000
                        long priceDelta = _random.Next(-5000, 5000); // +/- 0.5000
                        long price = priceBase + priceDelta;

                        uint qty = (uint)_random.Next(1, 101);

                        var payload = new NewOrderPayload(_orderIdCounter++, 999, price, qty, 1);

                        byte[] buffer = new byte[ProtocolConstants.FrameHeaderSize + ProtocolConstants.NewOrderPayloadSize];

                        MemoryMarshal.Write(buffer.AsSpan(0, ProtocolConstants.FrameHeaderSize), in header);
                        MemoryMarshal.Write(buffer.AsSpan(ProtocolConstants.FrameHeaderSize, ProtocolConstants.NewOrderPayloadSize), in payload);

                        await ws.SendAsync(buffer, WebSocketMessageType.Binary, true, stoppingToken);

                        // Send ~10 orders per second
                        await Task.Delay(100, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "BotWorker WebSocket error");
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }
    }
}
