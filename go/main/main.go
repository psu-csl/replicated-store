package main

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/replicant"
)

func main() {
	config := config.DefaultConfig(0, 1)
	replicant := replicant.NewReplicant(config)
	replicant.Start()
}
