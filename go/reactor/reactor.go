package reactor

import "encoding/json"
import "github.com/psu-csl/replicated-store/go/operation"
import "net"
import "log"
import "github.com/psu-csl/replicated-store/go/paxos"

type Reactor struct {
	listener   net.Listener
	me         int
	px         *paxos.Paxos
}

func NewReactor(servers []string, me int) *Reactor {
	reactor := Reactor{}
	reactor.me = me
	reactor.px = paxos.NewPaxos(servers, me)
	listener, err := net.Listen("tcp", servers[me])
	if err != nil {
		log.Fatalf("listener error: %v", err)
	}
	reactor.listener = listener
	return &reactor
}

func (r *Reactor) listen() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			//log.Printf("accept error: %v", err)
			break
		}
		go r.handleClientRequest(conn)
	}
}

func (r *Reactor) handleClientRequest(conn net.Conn) {
	for {
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			reply := operation.CommandResult{
				IsSuccess: false,
				Value:     []byte(""),
				Error:     err.Error(),
			}
			replyByte, err := json.Marshal(reply)
			if err != nil {
				log.Printf("json marshal error: %v", err)
				return
			}
			conn.Write(replyByte)
			break
		}
		cmd := &operation.Command{}
		err = json.Unmarshal(buffer[:n], cmd)
		if err != nil {
			log.Printf("json unmarshal error onserver: %v", err)
			continue
		}
		go r.px.Start(*cmd, conn)
	}
}

func (r *Reactor) Run() {
	go r.listen()
}

func (r *Reactor) Close() {
	r.listener.Close()
}