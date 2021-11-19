package reactor

import (
	"github.com/psu-csl/replicated-store/go/operation"
	"strconv"
	"testing"
	"time"
)
import "github.com/stretchr/testify/assert"

var servers []*Reactor
var clients []*Client

func setup(numClients int) {
	serverAddrs := []string {
		"localhost:8080",
	}
	servers = make([]*Reactor, len(serverAddrs))
	for i, _ := range serverAddrs {
		servers[i] = NewReactor(serverAddrs, i)
		servers[i].Run()
	}

	clients = make([]*Client, numClients)
	for i, _ := range clients {
		clients[i] = NewClient(serverAddrs, 0)
	}
}

func clear() {
	for _, client := range clients {
		client.Close()
	}
	for _, server := range servers {
		server.Close()
	}
	time.Sleep(1000 * time.Millisecond)
}

func TestBasic (t *testing.T) {
	numClients := 1
	setup(numClients)
	defer clear()
	client := clients[0]

	reply, err := client.Put([]byte("testKey"), []byte("testVal"))
	expected := &operation.CommandResult{
		CommandID: 1,
		IsSuccess: true,
		Value:     []byte(""),
		Error:     "",
	}
	if assert.Nil(t, err, "got error on client side: %v", err) {
		assert.Equalf(t, expected, reply, "PUT result expected %v, but got %v", expected, reply)
	}

	reply, err = client.Get([]byte("testKey"))
	expected = &operation.CommandResult{
		CommandID: 2,
		IsSuccess: true,
		Value:     []byte("testVal"),
		Error:     "",
	}
	if assert.Nil(t, err, "got error on client side: %v", err) {
		assert.Equalf(t, expected, reply, "GET result expected %v, but got %v", expected, reply)
	}

	reply, err = client.Get([]byte("non-exist"))
	expected = &operation.CommandResult{
		CommandID: 3,
		IsSuccess: false,
		Value:     []byte(""),
		Error:     "item not found",
	}
	if assert.Nil(t, err, "got error on client side: %v", err) {
		assert.Equalf(t, expected, reply, "GET non-existed result expected %v, but got %v", expected, reply)
	}


	reply, err = client.Delete([]byte("testKey"))
	expected = &operation.CommandResult{
		CommandID: 4,
		IsSuccess: true,
		Value:     []byte(""),
		Error:     "",
	}
	if assert.Nil(t, err, "got error on client side: %v", err) {
		assert.Equalf(t, expected, reply, "Delete result expected %v, but got %v", expected, reply)
	}
	//clear()
}

func TestMultipleCmd (t *testing.T) {
	numClients := 1
	setup(numClients)
	defer clear()
	client := clients[0]

	numCmds := 20
	for i := 1; i <= numCmds; i++ {
		key := []byte(strconv.Itoa(i))
		val := key
		_, err := client.Put(key, val)
		assert.Nil(t, err, "expect no err during put, but got %v", err)
	}

	for i := 1; i <= numCmds; i++ {
		key := []byte(strconv.Itoa(i))
		val := key
		expected := &operation.CommandResult{
			CommandID: int64(i) + 20,
			IsSuccess: true,
			Value:     []byte(val),
			Error:     "",
		}
		reply, err := client.Get(key)
		if assert.Nil(t, err, "got error on client side: %v", err) {
			assert.Equalf(t, expected, reply, "GET result expected %v, but got %v", expected, reply)
		}
	}
}

func TestMultipleClients (t *testing.T) {
	numClients := 3
	setup(numClients)
	defer clear()

	clientDone := make(chan bool, len(clients))
	numCmds := 10
	for i, client := range clients {
		go func(clientIndex int, client *Client) {
			for i := 1; i <= numCmds; i++ {
				key := []byte(strconv.Itoa(clientIndex* 10 + i))
				val := key
				_, err := client.Put(key, val)
				assert.Nil(t, err, "expect no err during put, but got %v", err)
			}
			clientDone <- true
		}(i, client)
	}

	numDone := 0
	for numDone < len(clients) {
		<-clientDone
		numDone++
	}
	time.Sleep(1000 * time.Millisecond)

	numReads := len(clients) * 10
	for i := 1; i <= numReads; i++ {
		key := []byte(strconv.Itoa(i))
		val := key
		reply, err := clients[0].Get(key)
		if assert.Nil(t, err, "got error on client side: %v", err) {
			assert.Equalf(t, val, reply.Value, "GET result expected %v, but got %v", val, reply)
		}
	}
}