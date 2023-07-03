package main

import (
	"flag"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/replicant"
	logger "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	id := flag.Int64("id", 0, "peer id")
	debug := flag.Bool("d", false, "enable debug logging")
	configPath := flag.String("c", "../c++/config.json", "config path")
	flag.Parse()

	if *debug {
		logger.SetLevel(logger.InfoLevel)
	} else {
		logger.SetLevel(logger.ErrorLevel)
	}

	cfg, err := config.LoadConfig(*id, *configPath)
	if err != nil {
		logger.Panic(err)
	}

	replicant := replicant.NewReplicant(cfg)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		replicant.Stop()
	}()
	replicant.Start()
}
