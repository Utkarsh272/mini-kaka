package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
	"github.com/Utkarsh272/mini-kafka/internal/server"
)

func main() {
	addr := flag.String("addr", ":9092", "TCP address to listen on")
	dataDir := flag.String("data-dir", "/tmp/mini-kafka", "Root directory for partition log files and metadata db")
	nodeID := flag.Int("node-id", 1, "Broker node ID")
	host := flag.String("host", "localhost", "Advertised hostname (returned in Metadata responses)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		slog.Error("create data dir", "err", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(int32(*nodeID), *host, 9092, *dataDir)
	if err != nil {
		slog.Error("init broker", "err", err)
		os.Exit(1)
	}
	defer b.Close()

	h := server.NewHandler(b)
	srv := server.NewServer(*addr, h)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutting down broker")
		srv.Close()
	}()

	slog.Info("starting mini-kafka broker", "addr", *addr, "data-dir", *dataDir, "node-id", *nodeID)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("broker error", "err", err)
		os.Exit(1)
	}
}
