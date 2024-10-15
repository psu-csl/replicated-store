package main

import (
	"flag"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/replicant"
	logger "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	id := flag.Int64("id", 0, "peer id")
	debug := flag.Bool("d", false, "enable debug logging")
	configPath := flag.String("c", "config/config.json", "config path")
	join := flag.Bool("j", false, "join peer")
	leaderAddr := flag.String("l", "", "peer address")
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

	replicant := replicant.NewReplicant(cfg, *join)

	if *join {
		if *leaderAddr == "" {
			panic("no leader address")
		}
		replicant.StartRpcServer()
		conn, err := net.Dial("tcp", *leaderAddr)
		if err != nil {
			panic(err)
		}

		nodeAddr := cfg.Peers[int(*id)]
		msg := "add " + strconv.Itoa(int(*id)) + " " + nodeAddr + "\n"
		_, err = conn.Write([]byte(msg))
		if err != nil {
			panic(err)
		}

		reply := make([]byte, 1024)
		_, err = conn.Read(reply)
		if err != nil {
			panic(err)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		replicant.Stop()
	}()
	statusChan := make(chan os.Signal, 1)
	signal.Notify(statusChan, syscall.SIGTSTP)
	go func() {
		for {
			<-statusChan
			//replicant.Monitor()
			replicant.TriggerElection()
		}
	}()
	replicant.Start()
	logger.Infoln("graceful shutdown")
}
