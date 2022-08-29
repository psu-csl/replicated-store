package main

import (
	"flag"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/replicant"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	const numPeers = 3
	id := flag.Int64("id", 0, "peer id")
	flag.Parse()

	config := config.DefaultConfig(*id, numPeers)
	replicant := replicant.NewReplicant(config)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<- signalChan
		replicant.Stop()
	}()
	replicant.Start()
}
