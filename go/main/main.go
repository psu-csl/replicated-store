package main

import "github.com/psu-csl/replicated-store/go/replicant"

func main() {
	serverAddrs := []string{
		"localhost:8888",
	}
	replicant := replicant.NewReplicant(serverAddrs, 0)
	replicant.Start()
}
