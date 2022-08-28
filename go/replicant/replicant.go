package replicant

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"log"
	"net"
	"strconv"
	"strings"
)

type Replicant struct {
	id            int64
	numPeers      int64
	store         *store.MemKVStore
	log           *consensusLog.Log
	multipaxos    *multipaxos.Multipaxos
	ipPort        string
	listener      net.Listener
	clientManager *ClientManager
}

func NewReplicant(config config.Config) *Replicant {
	r := &Replicant{
		id:            config.Id,
		numPeers:      int64(len(config.Peers)),
		store:         store.NewMemKVStore(),
		log:           consensusLog.NewLog(),
		ipPort:        config.Peers[config.Id],
	}
	r.multipaxos = multipaxos.NewMultipaxos(config, r.log)
	r.clientManager = NewClientManager(r.id, r.numPeers, r.multipaxos)

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
		panic("no : in the listener port")
	}
	pos += 1
	port, err := strconv.Atoi(r.ipPort[pos:])
	if err != nil {
		panic("parsing listener port failed")
	}
	port += 1

	listener, err := net.Listen("tcp", ":" + strconv.Itoa(port))
	if err != nil {
		log.Fatalf("listener error: %v", err)
	}
	log.Printf("replicant %v starting server at port %v\n", r.id, port)
	r.listener = listener
	r.AcceptClient()
}

func (r *Replicant) StopServer() {
	r.listener.Close()
	r.clientManager.StopAll()
}

func (r *Replicant) AcceptClient() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			log.Println(err)
			break
		}
		r.clientManager.Start(conn)
	}
}

func (r *Replicant) StartExecutorThread() {
	go r.executorThread()
}

func (r *Replicant) StopExecutorThread() {
	r.log.Stop()
}

func (r *Replicant) executorThread() {
	for {
		clientId, result := r.log.Execute(r.store)
		if result == nil {
			break
		}
		client := r.clientManager.Get(clientId)
		if client != nil {
			client.Write(result.Value)
		}
	}
}

