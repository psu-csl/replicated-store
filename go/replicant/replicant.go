package replicant

import (
	"bufio"
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"log"
	"net"
	"net/textproto"
)

type Replicant struct {
	log           *consensusLog.Log
	mp            *multipaxos.Multipaxos
	store         *store.MemKVStore
	id            int64
	nextClientId  int64
	numPeers      int64
	clientSockets map[int64]net.Conn
	clientPort    string
	listener      net.Listener

}

func NewReplicant(config config.Config) *Replicant {
	replicant := Replicant{}

	replicant.log = consensusLog.NewLog()
	replicant.mp = multipaxos.NewMultipaxos(config, replicant.log)
	replicant.store = store.NewMemKVStore()
	replicant.id = config.Id
	replicant.nextClientId = config.Id
	replicant.numPeers = int64(len(config.Peers))
	replicant.clientSockets = make(map[int64]net.Conn)
	replicant.clientPort = config.ClientPorts[replicant.id]

	listener, err := net.Listen("tcp", replicant.clientPort)
	if err != nil {
		log.Fatalf("listener error: %v", err)
	}
	replicant.listener = listener
	return &replicant
}

func (r *Replicant) Start() {
	go r.executorThread()
	go r.mp.Start()
	for {
		clientId := r.NextClientId()
		conn, err := r.listener.Accept()
		if err != nil {
			break
		}
		r.clientSockets[clientId] = conn
		go r.handleClient(clientId)
	}
}

func (r *Replicant) handleClient(clientId int64) {
	client, isExist := r.clientSockets[clientId]
	if !isExist {
		panic("no client connetion existed")
	}

	for {
		command, err := r.readCommand(client)
		if err != nil {
			reply := store.Result{
				Ok:    false,
				Value: "",
			}
			r.respond(client, reply)
			break
		}
		r.Replicate(command, clientId)
	}
}

func (r *Replicant) executorThread() {
	for {
		clientId, result := r.log.Execute(r.store)
		if conn, isExist := r.clientSockets[clientId]; isExist {
			r.respond(conn, result)
		}
	}
}

func (r *Replicant) readCommand(conn net.Conn) (*pb.Command, error) {
	line, err := r.readLine(conn)
	if err != nil {
		return nil, err
	}
	var command pb.Command
	if line[:3] == "get" {
		command.Type = pb.CommandType_GET
		command.Key = line[4:]
	} else if line[:3] == "del" {
		command.Type = pb.CommandType_DEL
		command.Key = line[4:]
	} else {
		command.Type = pb.CommandType_PUT
		command.Key = line[4:]
		//TODO: set value
	}
	return &command, nil
}

func (r *Replicant) readLine(conn net.Conn) (string, error) {
	reader := textproto.NewReader(bufio.NewReader(conn))
	line, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	return line, nil
}

func (r *Replicant) Replicate(command *pb.Command, clientId int64) {
	result := r.mp.Replicate(command, clientId)
	if result.Type == multipaxos.Ok {
		return
	}

	client, isExisted := r.clientSockets[clientId]
	if !isExisted {
		panic("no client connetion existed")
	}
	if result.Type == multipaxos.Retry {
		//TODO: write back retry
	} else {
		if result.Type != multipaxos.SomeElseLeader{
			panic("result type is not someElseLeader")
		}
		//TODO: send back new leader id
		client.Close()
		delete(r.clientSockets, clientId)
	}
}

func (r *Replicant) respond(conn net.Conn, result store.Result) {
	respByte, err := json.Marshal(result)
	if err != nil {
		log.Printf("json marshal error on server: %v", err)
	}
	respByte = append(respByte, '\n')
	_, err = conn.Write(respByte)
	if err != nil {
		log.Printf("server write error: %v", err)
	}
}

func (r *Replicant) NextClientId() int64 {
	id := r.nextClientId
	r.nextClientId += r.numPeers
	return id
}

func (r *Replicant) Close() {
	r.listener.Close()
	r.mp.Stop()
}
