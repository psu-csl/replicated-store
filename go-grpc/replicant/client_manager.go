package replicant

import (
	"github.com/psu-csl/replicated-store/go/multipaxos"
	logger "github.com/sirupsen/logrus"
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

func (cm *ClientManager) Start(socket net.Conn) {
	id := cm.NextClientId()
	client := NewClient(id, socket, cm.multipaxos, cm)

	cm.mu.Lock()
	cm.clients[id] = client
	cm.mu.Unlock()
	logger.Infof("client_manager started client %v\n", id)
	client.Start()
}

func (cm *ClientManager) Get(id int64) *Client {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	client, ok := cm.clients[id]
	if !ok {
		return nil
	}
	return client
}

func (cm *ClientManager) Stop(id int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	logger.Infof("client_manager stopped client %v\n", id)
	client, ok := cm.clients[id]
	if !ok {
		panic("no client to stop")
	}
	client.Stop()
	delete(cm.clients, id)
}

func (cm *ClientManager) StopAll() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for id, client := range cm.clients {
		logger.Infof("client_manager stopping all clients %v\n", id)
		client.Stop()
		delete(cm.clients, id)
	}
}

func (cm *ClientManager) NumClients() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return len(cm.clients)
}
