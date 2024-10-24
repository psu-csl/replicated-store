package replicant

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Replicant struct {
	id            int64
	log           *consensusLog.Log
	multipaxos    *multipaxos.Multipaxos
	ipPort        string
	acceptor      net.Listener
	clientManager *ClientManager
	wg            *sync.WaitGroup
}

func NewReplicant(config config.Config, join bool) *Replicant {
	r := &Replicant{
		id:     config.Id,
		ipPort: config.Peers[config.Id],
		wg:     &sync.WaitGroup{},
	}
	r.log = consensusLog.NewLog(kvstore.CreateStore(config))
	r.multipaxos = multipaxos.NewMultipaxos(r.log, config, join)
	r.clientManager = NewClientManager(r.id, int64(len(config.Peers)), r.multipaxos)
	return r
}

func (r *Replicant) Start() {
	r.multipaxos.Start()
	r.StartExecutorThread()
	r.StartServer()
	r.wg.Wait()
}

func (r *Replicant) Stop() {
	r.StopServer()
	r.multipaxos.Stop()
	r.StopExecutorThread()
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
	r.wg.Add(1)
	go r.executorThread()
}

func (r *Replicant) StopExecutorThread() {
	logger.Infof("%v stopping executor thread\n", r.id)
	r.log.Stop()
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
				client.Write(result.Value)
			} else if instance.ClientId == -1 {
				values := strings.Split(instance.Command.Value, " ")
				posVarSum, _ := strconv.ParseFloat(values[0], 64)
				negVarSum, _ := strconv.ParseFloat(values[1], 64)
				r.multipaxos.TriggerElection(instance.GetBallot(), posVarSum,
					negVarSum)
			}
		}
	}
	r.wg.Done()
}

func (r *Replicant) AcceptClient() {
	for {
		conn, err := r.acceptor.Accept()
		if err != nil {
			logger.Info(err)
			break
		}
		r.clientManager.Start(conn)
	}
}

func (r *Replicant) Monitor() {
	r.multipaxos.Monitor()
}

func (r *Replicant) TriggerElection() {
	r.multipaxos.TriggerElection(1000, 0, 0)
}
