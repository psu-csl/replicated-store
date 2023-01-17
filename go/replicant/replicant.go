package replicant

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	logger "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
)

type Replicant struct {
	id            int64
	log           *consensusLog.Log
	multipaxos    *multipaxos.Multipaxos
	ipPort        string
	acceptor      net.Listener
	clientManager *ClientManager
}

func NewReplicant(config config.Config) *Replicant {
	r := &Replicant{
		id:       config.Id,
		ipPort:   config.Peers[config.Id],
	}
	r.log = consensusLog.NewLog(kvstore.CreateStore(config))
	r.multipaxos = multipaxos.NewMultipaxos(r.log, config)
	r.clientManager = NewClientManager(r.id, int64(len(config.Peers)), r.multipaxos)
	return r
}

func (r *Replicant) Start() {
	r.multipaxos.Start()
	r.StartExecutorThread()
	r.StartServer()
}

func (r *Replicant) Stop() {
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

	acceptor, err := net.Listen("tcp", ":" + strconv.Itoa(port))
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

func (r *Replicant) executorThread() {
	for {
		id, result := r.log.Execute()
		if result == nil {
			break
		}
		client := r.clientManager.Get(id)
		if client != nil {
			client.Write(result.Value)
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
