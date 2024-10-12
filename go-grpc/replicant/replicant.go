package replicant

import (
	"bufio"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	CLIENT_COUNT int64
	INSERT_COUNT int64
	QUEUE_SIZE   int = 100
)

type Replicant struct {
	id            int64
	log           *consensusLog.Log
	multipaxos    *multipaxos.Multipaxos
	ipPort        string
	acceptor      net.Listener
	clientManager *ClientManager
	sampleRate    int64
	sampleQueue   *Queue
	config        config.Config

	overloadedCount int
	normalCount     int
}

func NewReplicant(config config.Config, join bool) *Replicant {
	r := &Replicant{
		id:          config.Id,
		ipPort:      config.Peers[config.Id],
		sampleRate:  config.SampleInterval,
		sampleQueue: NewQueue(QUEUE_SIZE),
		config:      config,
	}
	r.log = consensusLog.NewLog(kvstore.CreateStore(config))
	r.multipaxos = multipaxos.NewMultipaxos(r.log, config, join)
	r.clientManager = NewClientManager(r.id, int64(len(config.Peers)),
		r.multipaxos, r.sampleRate)
	CLIENT_COUNT = config.ClientCount
	INSERT_COUNT = config.InsertCount
	return r
}

func (r *Replicant) Start() {
	r.StartMonitorThread()
	r.multipaxos.Start()
	r.StartExecutorThread()
	r.StartServer()
}

func (r *Replicant) Stop(outputPath string) {
	if outputPath != "" {
		r.OutputMap(outputPath)
	}
	r.StopServer()
	r.StopExecutorThread()
	r.multipaxos.Stop()
}

func (r *Replicant) StartServer() {
	pos := strings.Index(r.ipPort, ":")
	if pos == -1 {
		panic("no separator : in the acceptor port")
	}
	pos += 1
	port, err := strconv.Atoi(r.ipPort[pos:])
	if err != nil {
		panic("parsing acceptor port failed")
	}
	port += 1

	acceptor, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		logger.Fatalln(err)
	}
	logger.Infof("%v starting server at port %v\n", r.id, port)
	r.acceptor = acceptor
	r.AcceptClient()
}

func (r *Replicant) StopServer() {
	r.acceptor.Close()
	r.clientManager.StopAll()
}

func (r *Replicant) StartExecutorThread() {
	logger.Infof("%v starting executor thread\n", r.id)
	go r.executorThread()
}

func (r *Replicant) StopExecutorThread() {
	logger.Infof("%v stopping executor thread\n", r.id)
	r.log.Stop()
}

func (r *Replicant) StartMonitorThread() {
	go r.MonitorThread()
}

func (r *Replicant) StartRpcServer() {
	r.multipaxos.StartRPCServer()
}

func (r *Replicant) executorThread() {
	for {
		instance := r.log.ReadInstance()
		if instance == nil {
			break
		}
		if instance.Command.Type == pb.CommandType_ADDNODE || instance.
			Command.Type == pb.CommandType_DELNODE {
			r.multipaxos.Reconfigure(instance.Command)
			client := r.clientManager.Get(instance.ClientId)
			if client != nil {
				client.Write("joined")
			}
		} else {
			id, result := r.log.Execute(instance)
			client := r.clientManager.Get(id)
			if client != nil {
				rId := instance.GetCommand().GetReqId()
				if rId%r.sampleRate == 0 {
					tp := time.Now().UnixNano()
					latency := client.ComputeLatency(tp)
					r.sampleQueue.Append(latency)
				}
				client.Write(result.Value)
			}
		}
	}
}

func (r *Replicant) AcceptClient() {
	for {
		conn, err := r.acceptor.Accept()
		if err != nil {
			logger.Error(err)
			break
		}
		r.clientManager.Start(conn)
	}
}

func (r *Replicant) MonitorThread() {
	var (
		median = 0
		p99    = 1
		//p999   = 2
	)
	for {
		stats := r.sampleQueue.Stats(50, 99, 99.9)
		ratio := stats[p99] / stats[median]
		if ratio >= r.config.SlowThreshold {
			if r.overloadedCount == 0 {
				r.overloadedCount = 1
				r.normalCount = 0
			} else if r.overloadedCount == 1 {
				//Replicate a new command
			}
		} else if r.overloadedCount > 0 && ratio < r.config.SlowThreshold {
			if r.normalCount == 0 {
				r.normalCount = 1
			} else if r.normalCount == 1 {
				r.overloadedCount = 0
				// reset and see if enough time elapses
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (r *Replicant) Monitor() {
	r.multipaxos.Monitor()
}

func (r *Replicant) TriggerElection() {
	r.multipaxos.TriggerElection()
}

func (r *Replicant) OutputMap(path string) {
	file, err := os.Create(path + ".dat")
	defer file.Close()
	if err != nil {
		logger.Error(err)
		return
	}
	writer := bufio.NewWriter(file)
	writer.Flush()
}
