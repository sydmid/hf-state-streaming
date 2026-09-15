#!/bin/bash
make setup-uds
cd edge-go && ./server > /tmp/edge.log 2>&1 &
EDGE_PID=$!
sleep 1

cd engine-dotnet/src/Engine.Core && dotnet run > /tmp/engine.log 2>&1 &
ENGINE_PID=$!
sleep 1

cd engine-dotnet/src/Demo.WebUi && dotnet run > /tmp/webui.log 2>&1 &
WEBUI_PID=$!
sleep 4

echo "Demo started."
