package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hf-state-streaming/edge-go/internal/gateway"
	"hf-state-streaming/edge-go/internal/ipc"

	"github.com/lxzan/gws"
)

func main() {
	wsAddr := flag.String("ws-addr", ":8080", "WebSocket listening address")
	ingressSock := flag.String("ingress-sock", "/tmp/engine_ingress.sock", "Path to .NET engine ingress UDS")
	egressSock := flag.String("egress-sock", "/tmp/engine_egress.sock", "Path to .NET engine egress UDS")
	flag.Parse()

	log.Println("[edge-go] Initializing High-Frequency Ingestion & Fanout Tier...")

	ipcClient := ipc.NewUDSClient(*ingressSock)
	go func() {
		for {
			if err := ipcClient.Connect(); err == nil {
				log.Printf("[edge-go] Connected to .NET engine ingress UDS: %s", *ingressSock)
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	egressBroadcaster := ipc.NewEgressBroadcaster(*egressSock)
	if err := egressBroadcaster.Start(); err != nil {
		log.Fatalf("[edge-go] Failed to start egress listener: %v", err)
	}
	log.Printf("[edge-go] Egress broadcaster listening on UDS: %s", *egressSock)

	handler := gateway.NewGatewayHandler(ipcClient, egressBroadcaster)
	server := gws.NewServer(handler, &gws.ServerOption{
		ParallelEnabled: false,
		Recovery:        gws.BuiltinRecovery,
		ReadBufferSize:  65536,
		WriteBufferSize: 65536,
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		socket, err := server.Upgrade(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		go socket.ReadLoop()
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		conns, frames, bytesIn, drops := handler.Stats()
		egressEvents, egressBytes := egressBroadcaster.Stats()
		ipcSentFrames, ipcSentBytes := ipcClient.Stats()
		log.Printf("[stats] Conns: %d | Inbound: %d frames (%d B) | Drops: %d | IPC Sent: %d frames (%d B) | Egress: %d events (%d B)",
			conns, frames, bytesIn, drops, ipcSentFrames, ipcSentBytes, egressEvents, egressBytes)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("[edge-go] WebSocket edge gateway listening on %s/ws", *wsAddr)
		if err := http.ListenAndServe(*wsAddr, nil); err != nil {
			serverErr <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Printf("[edge-go] Received signal %v, terminating...", sig)
	case err := <-serverErr:
		log.Fatalf("[edge-go] Server fatal error: %v", err)
	}

	_ = egressBroadcaster.Stop()
	_ = ipcClient.Close()
	log.Println("[edge-go] Edge gateway shutdown completed.")
}
