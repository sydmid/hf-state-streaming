#!/usr/bin/env bash
set -euo pipefail

INGRESS_SOCK="/tmp/engine_ingress.sock"
EGRESS_SOCK="/tmp/engine_egress.sock"

echo "[setup_uds] Cleaning up stale Unix Domain Sockets..."
rm -f "${INGRESS_SOCK}" "${EGRESS_SOCK}"

echo "[setup_uds] Ready for socket creation: ${INGRESS_SOCK}, ${EGRESS_SOCK}"
