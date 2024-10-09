package replicant

import (
	"bufio"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	logger "github.com/sirupsen/logrus"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
)

type ClientManager struct {
	nextId     int64
	numPeers   int64
	multipaxos *multipaxos.Multipaxos
	mu         sync.Mutex
	clients    map[int64]*Client
	sampleRate int64
}

func NewClientManager(id int64,
	numPeers int64,
	mp *multipaxos.Multipaxos,
	sampleRate int64) *ClientManager {
	cm := &ClientManager{
		nextId:     id,
		numPeers:   numPeers,
		multipaxos: mp,
		clients:    make(map[int64]*Client),
		sampleRate: sampleRate,
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
	//delete(cm.clients, id)
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

func (cm *ClientManager) SampleRate() int64 {
	return cm.sampleRate
}

func (cm *ClientManager) NumPeers() int64 {
	return cm.numPeers
}

func (cm *ClientManager) OutputMap(path string) {
	cm.mu.Lock()
	latenciesMap := make(map[int64]int64)
	keys := make([]int64, 0)
	for _, client := range cm.clients {
		lm := client.latencyMap.GetMap()
		for id, tp := range lm {
			if _, ok := latenciesMap[id]; ok {
				logger.Errorf("duplicated rId: %v\n", id)
			} else if id > INSERT_COUNT {
				latenciesMap[id] = tp
				keys = append(keys, id)
			}
		}
	}
	cm.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	file, err := os.Create(path + ".dat")
	defer file.Close()
	if err != nil {
		logger.Error(err)
		return
	}
	writer := bufio.NewWriter(file)
	for _, rId := range keys {
		_, err = writer.WriteString(strconv.FormatInt(rId, 10) +
			" " + strconv.FormatInt(latenciesMap[rId], 10) + "\n")
		if err != nil {
			logger.Error(err)
			return
		}
	}
	writer.Flush()
}
