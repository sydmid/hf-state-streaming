// Constants for protocol parsing
const FrameMagic = 0x534D;
const MsgTypeNewOrder = 0x01;
const MsgTypeCancelOrder = 0x02;
const MsgTypeOrderAck = 0x03;
const MsgTypeOrderExecuted = 0x04;
const MsgTypeOrderBookL2Delta = 0x05;

const SideBuy = 0x00;
const SideSell = 0x01;

// Internal state for order book
// Map of price -> { askSize: number, bidSize: number }
const orderBook = new Map();
const maxTradesToShow = 20;

function formatPrice(priceInt) {
    return (priceInt / 10000).toFixed(4);
}

function updateOrderBookUI() {
    const tbody = document.getElementById('orderBookBody');
    tbody.innerHTML = '';

    // Sort prices descending
    const sortedPrices = Array.from(orderBook.keys()).sort((a, b) => b - a);

    for (const price of sortedPrices) {
        const level = orderBook.get(price);

        // Remove empty levels
        if (level.askSize === 0 && level.bidSize === 0) {
            orderBook.delete(price);
            continue;
        }

        const tr = document.createElement('tr');

        const askTd = document.createElement('td');
        askTd.className = 'text-danger';
        askTd.textContent = level.askSize > 0 ? level.askSize : '';

        const priceTd = document.createElement('td');
        priceTd.className = 'font-weight-bold';
        priceTd.textContent = formatPrice(price);

        const bidTd = document.createElement('td');
        bidTd.className = 'text-success';
        bidTd.textContent = level.bidSize > 0 ? level.bidSize : '';

        tr.appendChild(askTd);
        tr.appendChild(priceTd);
        tr.appendChild(bidTd);

        tbody.appendChild(tr);
    }
}

function addTradeUI(tradeId, price) {
    const tbody = document.getElementById('tradesBody');
    const tr = document.createElement('tr');

    const idTd = document.createElement('td');
    idTd.textContent = tradeId.toString();

    const priceTd = document.createElement('td');
    priceTd.textContent = formatPrice(price);

    tr.appendChild(idTd);
    tr.appendChild(priceTd);

    tbody.insertBefore(tr, tbody.firstChild);

    if (tbody.children.length > maxTradesToShow) {
        tbody.removeChild(tbody.lastChild);
    }
}

function connectWebSocket() {
    const statusDiv = document.getElementById('connectionStatus');
    const ws = new WebSocket('ws://127.0.0.1:8080/ws');
    ws.binaryType = 'arraybuffer';

    ws.onopen = function() {
        statusDiv.className = 'alert alert-success';
        statusDiv.textContent = 'Connected to Engine Edge Server';
    };

    ws.onclose = function() {
        statusDiv.className = 'alert alert-danger';
        statusDiv.textContent = 'Disconnected. Reconnecting in 3s...';
        setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = function(err) {
        console.error('WebSocket Error:', err);
    };

    ws.onmessage = function(event) {
        const buffer = event.data;
        if (buffer.byteLength < 8) return; // Need at least header

        const dv = new DataView(buffer);
        let offset = 0;

        while (offset + 8 <= buffer.byteLength) {
            const magic = dv.getUint16(offset, true);
            if (magic !== FrameMagic) {
                console.error('Invalid Magic Frame');
                break;
            }

            const msgType = dv.getUint8(offset + 2);
            const flags = dv.getUint8(offset + 3);
            const payloadLen = dv.getUint16(offset + 4, true);

            offset += 8; // move past header

            if (offset + payloadLen > buffer.byteLength) {
                break; // incomplete payload
            }

            if (msgType === MsgTypeOrderBookL2Delta) {
                if (payloadLen >= 24) {
                    const assetId = dv.getUint32(offset, true);
                    const priceBig = dv.getBigInt64(offset + 8, true);
                    const newQuantity = dv.getUint32(offset + 16, true);
                    const side = dv.getUint8(offset + 20);

                    const price = Number(priceBig);

                    if (!orderBook.has(price)) {
                        orderBook.set(price, { askSize: 0, bidSize: 0 });
                    }

                    const level = orderBook.get(price);
                    if (side === SideBuy) {
                        level.bidSize = newQuantity;
                    } else {
                        level.askSize = newQuantity;
                    }
                }
            } else if (msgType === MsgTypeOrderExecuted) {
                if (payloadLen >= 32) {
                    const tradeId = dv.getBigUint64(offset, true);
                    const priceBig = dv.getBigInt64(offset + 24, true);

                    addTradeUI(tradeId, Number(priceBig));
                }
            }

            offset += payloadLen; // move to next frame
        }

        // Batch UI updates per message block (which could contain multiple frames)
        updateOrderBookUI();
    };
}

// Start connection on load
document.addEventListener('DOMContentLoaded', connectWebSocket);
