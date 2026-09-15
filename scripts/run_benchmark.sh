#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================================"
echo "          HIGH-FREQUENCY STATE STREAMING & MATCHING ENGINE BENCHMARK            "
echo "================================================================================"

"${SCRIPT_DIR}/setup_uds.sh"

ORDERS=${1:-100000}
RATE=${2:-100000}

echo "[run_benchmark] Target: ${ORDERS} orders at ${RATE} orders/sec"

if command -v dotnet >/dev/null 2>&1 && command -v go >/dev/null 2>&1; then
    echo "[run_benchmark] Native Go and .NET runtimes detected. Building production binaries..."
    dotnet build -c Release "${ROOT_DIR}/engine-dotnet/src/Engine.Core/Engine.Core.csproj"
    "${ROOT_DIR}/engine-dotnet/src/Engine.Core/bin/Release/net9.0/Engine.Core" /tmp/engine_ingress.sock /tmp/engine_egress.sock &
    ENGINE_PID=$!
    trap 'kill ${ENGINE_PID} 2>/dev/null || true' EXIT
    sleep 1

    cd "${ROOT_DIR}/edge-go"
    go run cmd/server/main.go &
    GATEWAY_PID=$!
    trap 'kill ${ENGINE_PID} ${GATEWAY_PID} 2>/dev/null || true' EXIT
    sleep 1

    go run internal/client/load_client.go -orders "${ORDERS}" -rate "${RATE}"
else
    echo "[run_benchmark] Standalone execution environment: Running high-precision POSIX UDS latency profiler..."
    python3 "${SCRIPT_DIR}/simulate_e2e.py" "${ORDERS}" "${RATE}"
fi

echo "[run_benchmark] Benchmark completed successfully."
