.PHONY: all clean setup-uds test-proto test-go test-dotnet benchmark-dotnet benchmark-e2e profile-latency

all: test-proto setup-uds

setup-uds:
	@chmod +x scripts/setup_uds.sh
	@scripts/setup_uds.sh

test-proto:
	@echo "==> Verifying binary protocol packing, struct sizes, and field offsets..."
	@gcc -Wall -Wextra -O2 proto/test_layout.c -o proto/test_layout
	@proto/test_layout
	@rm -f proto/test_layout

test-go:
	@echo "==> Running Go unit and allocation benchmarks (target: 0 allocs)..."
	@cd edge-go && go test -v -benchmem ./...

test-dotnet:
	@echo "==> Running .NET 9 xUnit test suite..."
	@cd engine-dotnet && dotnet test tests/Engine.Tests/Engine.Tests.csproj

build-dotnet-aot:
	@echo "==> Compiling .NET 9 Matching Engine with Native AOT..."
	@cd engine-dotnet && dotnet publish src/Engine.Core/Engine.Core.csproj -c Release -r linux-x64 --self-contained

benchmark-dotnet:
	@echo "==> Running BenchmarkDotNet (Zero-Allocation Verification)..."
	@cd engine-dotnet && dotnet run -c Release --project benchmarks/Engine.Benchmarks/Engine.Benchmarks.csproj

benchmark-e2e: setup-uds
	@echo "==> Executing End-to-End Tick-to-Trade Latency Benchmark (100k orders/sec)..."
	@scripts/run_benchmark.sh 100000 100000

profile-latency: setup-uds
	@echo "==> Profiling Latency Distribution (P50, P90, P99, P99.9, Jitter)..."
	@scripts/profile_latency.sh

clean:
	@echo "==> Cleaning up build artifacts and domain sockets..."
	@rm -f /tmp/engine_ingress.sock /tmp/engine_egress.sock
	@rm -rf engine-dotnet/src/Engine.Core/bin engine-dotnet/src/Engine.Core/obj
	@rm -rf engine-dotnet/benchmarks/Engine.Benchmarks/bin engine-dotnet/benchmarks/Engine.Benchmarks/obj
	@rm -rf engine-dotnet/tests/Engine.Tests/bin engine-dotnet/tests/Engine.Tests/obj
	@rm -f proto/test_layout
