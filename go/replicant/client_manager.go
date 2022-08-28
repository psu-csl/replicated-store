package replicant

import (
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos"
	"net"
	"sync"
)

type ClientManager struct {
	nextId     int64
	numPeers   int64
	multipaxos *multipaxos.Multipaxos
	mu         sync.Mutex
	clients    map[int64]*Client
}

func NewClientManager(id int64,
				      numPeers int64,
	                  mp *multipaxos.Multipaxos) *ClientManager {
	cm := &ClientManager{
		nextId:     id,
		numPeers:   numPeers,
		multipaxos: mp,
		clients:    make(map[int64]*Client),
	}
	return cm
}

func (cm *ClientManager) NextClientId() int64 {
	id := cm.nextId
	cm.nextId += cm.numPeers
	return id
}

func (cm *ClientManager) Start(conn net.Conn) {
	id := cm.NextClientId()
	client := NewClient(id, conn, cm.multipaxos, cm)

	cm.mu.Lock()
	cm.clients[id] = client
	cm.mu.Unlock()

	client.Start()
}

func (cm *ClientManager) Get(id int64) *Client {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if client, ok := cm.clients[id]; ok {
		return client
	}
	return nil
}

func (cm *ClientManager) Stop(id int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if client, ok := cm.clients[id]; ok {
		client.Stop()
		delete(cm.clients, id)
		return
	}
	panic("no client to stop")
}

func (cm *ClientManager) StopAll() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for id, client := range cm.clients {
		client.Stop()
		delete(cm.clients, id)
	}
}
