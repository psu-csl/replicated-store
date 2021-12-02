package replicant

import (
	"bufio"
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/psu-csl/replicated-store/go/paxos"
	"github.com/psu-csl/replicated-store/go/store"
	"log"
	"net"
	"net/textproto"
)

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
	reader := textproto.NewReader(bufio.NewReader(conn))
	for {
		//buffer := make([]byte, 1056)
		buffer, err := reader.ReadLineBytes()
		if err != nil {
			reply := command.CommandResult{
				IsSuccess: false,
				Value:     "",
			}
			replyByte, err := json.Marshal(reply)
			replyByte = append(replyByte, '\n')
			if err != nil {
				log.Printf("json marshal error: %v", err)
				return
			}
			conn.Write(replyByte)
			break
		}
		cmd := &command.Command{}
		err = json.Unmarshal(buffer[:], cmd)
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
			respByte = append(respByte, '\n')
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
