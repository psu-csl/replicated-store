package replicant

import (
	"bufio"
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/network"
	"net"
	"strings"
)

type Client struct {
	id           int64
	reader       *bufio.Reader
	writer       *bufio.Writer
	socket       net.Conn
	multipaxos   *multipaxos.Multipaxos
	manager      *ClientManager
	isFromClient bool
}

func NewClient(id int64, conn net.Conn, mp *multipaxos.Multipaxos,
	manger *ClientManager, isFromClient bool) *Client {
	client := &Client{
		id:           id,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		socket:       conn,
		multipaxos:   mp,
		manager:      manger,
		isFromClient: isFromClient,
	}
	return client
}

func (c *Client) Parse(request string) *pb.Command {
	substrings := strings.SplitN(strings.TrimRight(request, "\n"), " ", 3)
	if len(substrings) < 2 {
		return nil
	}
	commandType := substrings[0]
	key := substrings[1]

	command := &pb.Command{Key: key}

	if commandType == "get" {
		command.Type = pb.Get
	} else if commandType == "del" {
		command.Type = pb.Del
	} else if commandType == "put" {
		if len(substrings) != 3 {
			return nil
		}
		command.Type = pb.Put
		command.Value = substrings[2]
	} else {
		return nil
	}
	return command
}

func (c *Client) Start() {
	go c.Read()
}

func (c *Client) Stop() {
	c.socket.Close()
}

func (c *Client) Read() {
	for {
		request, err := c.reader.ReadString('\n')
		if err != nil {
			c.manager.Stop(c.id)
			return
		}
		c.handleRequest(request)
	}
}

func (c *Client) handleRequest(request string) {
	if c.isFromClient {
		c.handleClientRequest(request)
	} else {
		c.handlePeerRequest(request)
	}
}

func (c *Client) handleClientRequest(line string) {
	command := c.Parse(line)
	if command != nil {
		result := c.multipaxos.Replicate(command, c.id)
		if result.Type == multipaxos.Ok {
			return
		}
		if result.Type == multipaxos.Retry {
			c.Write("retry")
		} else {
			if result.Type != multipaxos.SomeElseLeader {
				panic("Result is not someone_else_leader")
			}
			c.Write("leader is ...")
		}
	} else {
		c.Write("bad command")
	}
}

func (c *Client) handlePeerRequest(line string) {
	var request pb.Message
	err := json.Unmarshal([]byte(line), &request)
	if err != nil {
		return
	}

	msg := []byte(request.Msg)
	go func() {
		switch pb.MessageType(request.Type) {
		case pb.PREPAREREQUEST:
			var prepareRequest pb.PrepareRequest
			json.Unmarshal(msg, &prepareRequest)
			prepareResponse := c.multipaxos.Prepare(prepareRequest)
			responseJson, _ := json.Marshal(prepareResponse)
			tcpMessage, _ := json.Marshal(pb.Message{
				Type:      uint8(pb.PREPARERESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			c.Write(string(tcpMessage))
		case pb.ACCEPTREQUEST:
			var acceptRequest pb.AcceptRequest
			json.Unmarshal(msg, &acceptRequest)
			acceptResponse := c.multipaxos.Accept(acceptRequest)
			responseJson, _ := json.Marshal(acceptResponse)
			tcpMessage, _ := json.Marshal(pb.Message{
				Type:      uint8(pb.ACCEPTRESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			c.Write(string(tcpMessage))
		case pb.COMMITREQUEST:
			var commitRequest pb.CommitRequest
			json.Unmarshal(msg, &commitRequest)
			commitResponse := c.multipaxos.Commit(commitRequest)
			responseJson, _ := json.Marshal(commitResponse)
			tcpMessage, _ := json.Marshal(pb.Message{
				Type:      uint8(pb.COMMITRESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			c.Write(string(tcpMessage))
		}
	}()
}

func (c *Client) Write(response string) {
	_, err := c.writer.WriteString(response + "\n")
	if err == nil {
		c.writer.Flush()
	}
}
