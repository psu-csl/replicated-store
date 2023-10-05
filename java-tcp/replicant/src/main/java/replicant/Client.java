package replicant;

import static multipaxos.network.Message.MessageType.ACCEPTRESPONSE;
import static multipaxos.network.Message.MessageType.COMMITRESPONSE;
import static multipaxos.network.Message.MessageType.PREPARERESPONSE;

import command.Command;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;
import multipaxos.network.AcceptRequest;
import multipaxos.network.AcceptResponse;
import multipaxos.network.CommitRequest;
import multipaxos.network.CommitResponse;
import multipaxos.network.Message;
import multipaxos.network.Message.MessageType;
import multipaxos.network.PrepareRequest;
import multipaxos.network.PrepareResponse;
import org.codehaus.jackson.map.ObjectMapper;

public class Client {

  private final long id;
  private final Channel socket;
  private final MultiPaxos multiPaxos;
  private final boolean isFromClient;

  public Client(long id, Channel socket, MultiPaxos multiPaxos,
      boolean isFromClient, ExecutorService threadPool) {
    this.id = id;
    this.socket = socket;
    this.multiPaxos = multiPaxos;
    this.isFromClient = isFromClient;
  }

  public static Command parse(String request) {
    if (request == null) {
      return null;
    }
    String[] tokens = request.split(" ");
    String command = tokens[0];
    String key = tokens[1];
    Command res = new Command();
    res.setKey(key);
    if ("get".equals(command)) {
      res.setCommandType(Command.CommandType.Get);
    } else if ("del".equals(command)) {
      res.setCommandType(Command.CommandType.Del);
    } else if ("put".equals(command)) {
      res.setCommandType(Command.CommandType.Put);
      String value = tokens[2];
      if (value == null) {
        return null;
      }
      res.setValue(value);
    } else {
      return null;
    }
    return res;
  }

  public void handleRequest(String msg) {
    if (isFromClient) {
      handleClientRequest(msg);
    } else {
      handlePeerRequest(msg);
    }
  }

  public void handleClientRequest(String msg) {
    var command = parse(msg);
    if (command != null) {
      var r = multiPaxos.replicate(command, id);
      if (r.type == MultiPaxosResultType.kOk) {
        socket.flush();
      } else if (r.type == MultiPaxosResultType.kRetry) {
        write("retry");
      } else {
        assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
        write("leader is ...");
      }
    } else {
      write("bad command");
    }
  }

  public void handlePeerRequest(String msg) {}

  public void write(String response) {
    socket.writeAndFlush(response + "\n");
  }
}
