package replicant

import (
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/command"
	"net"
	"time"
)

type Client struct {
	conn     net.Conn
	servers  []string
	serverID int
}

func NewClient(servers []string, prefID int) *Client {
	client := Client{
		servers:  servers,
		serverID: prefID,
	}
	client.dial()
	return &client
}

func (c *Client) Get(key string) (*command.CommandResult, error) {
	request := command.Command{
		Key:         key,
		Value:       "",
		CommandType: "Get",
	}
	return c.sendRequest(request)
}

func (c *Client) Put(key string, val string) (*command.CommandResult, error) {
	request := command.Command{
		Key:         key,
		Value:       val,
		CommandType: "Put",
	}
	return c.sendRequest(request)
}

func (c *Client) Delete(key string) (*command.CommandResult, error) {
	request := command.Command{
		Key:         key,
		Value:       "",
		CommandType: "Delete",
	}
	return c.sendRequest(request)
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) dial() {
	for {
		conn, err := net.Dial("tcp", c.servers[c.serverID])
		if err != nil {
			c.serverID = (c.serverID + 1) % len(c.servers)
			time.Sleep(1 * time.Second)
			continue
		}
		c.conn = conn
		break
	}
}

func (c *Client) sendRequest(request command.Command) (*command.CommandResult,
	error) {
	reqByte, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	_, err = c.conn.Write(reqByte)
	if err != nil {
		return nil, err
	}

	cmdResult := &command.CommandResult{}
	buffer := make([]byte, 1024)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(buffer[:n], cmdResult)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return cmdResult, nil
}