package replicant

import (
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/store"
)
import "github.com/psu-csl/replicated-store/go/command"
import "net"
import "log"
import "github.com/psu-csl/replicated-store/go/paxos"

type Replicant struct {
	listener net.Listener
	me       int
	px       *paxos.Paxos
}

func NewReplicant(servers []string, me int) *Replicant {
	reactor := Replicant{}
	reactor.me = me
	store := store.NewStore()
	reactor.px = paxos.NewPaxos(servers, me, store)
	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalf("listener error: %v", err)
	}
	reactor.listener = listener
	return &reactor
}

func (r *Replicant) Run() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			//log.Printf("accept error: %v", err)
			break
		}
		go r.handleClientRequest(conn)
	}
}

func (r *Replicant) handleClientRequest(conn net.Conn) {
	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			reply := command.CommandResult{
				IsSuccess: false,
				Value:     "",
			}
			replyByte, err := json.Marshal(reply)
			if err != nil {
				log.Printf("json marshal error: %v", err)
				return
			}
			conn.Write(replyByte)
			break
		}
		cmd := &command.Command{}
		err = json.Unmarshal(buffer[:n], cmd)
		if err != nil {
			log.Printf("json unmarshal error onserver: %v", err)
			continue
		}
		go func() {
			cmdResult := r.px.AgreeAndExecute(*cmd)
			// Socket writes back command result
			respByte, err := json.Marshal(cmdResult)
			if err != nil {
				log.Printf("json marshal error on server: %v", err)
			}
			_, err = conn.Write(respByte)
			if err != nil {
				log.Printf("server write error: %v", err)
			}
		}()
	}
}

func (r *Replicant) Close() {
	r.listener.Close()
}
