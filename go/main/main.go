package main

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/replicant"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config := config.DefaultConfig(0, 1)
	replicant := replicant.NewReplicant(config)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<- signalChan
		replicant.Stop()
	}()
	replicant.Start()
}
