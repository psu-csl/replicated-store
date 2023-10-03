package replicant;

import static multipaxos.network.Message.MessageType.ACCEPTRESPONSE;
import static multipaxos.network.Message.MessageType.COMMITRESPONSE;
import static multipaxos.network.Message.MessageType.PREPARERESPONSE;

import ch.qos.logback.classic.Logger;
import command.Command;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;
import multipaxos.network.AcceptRequest;
import multipaxos.network.CommitRequest;
import multipaxos.network.Message;
import multipaxos.network.PrepareRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

@Sharable
public class ClientManager extends SimpleChannelInboundHandler<String> {

  private long nextClientId;
  private final long numPeers;
  private final MultiPaxos multiPaxos;
  private final ReentrantLock mu = new ReentrantLock();
  private final HashMap<Long, Client> clients = new HashMap<>();
  private final boolean isFromClient;
  private final ExecutorService threadPool;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(
      ClientManager.class);
  private final AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf(
      "ClientID");

  public ClientManager(long id, long numPeers, MultiPaxos multiPaxos,
      boolean isFromClient, int threadPoolSize) {
    this.nextClientId = id;
    this.numPeers = numPeers;
    this.multiPaxos = multiPaxos;
    this.isFromClient = isFromClient;
    this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
  }

  private long nextClientId() {
    var id = nextClientId;
    nextClientId += numPeers;
    return id;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    mu.lock();
    var id = nextClientId();
    logger.debug("client joined " + ctx);
    ctx.channel().attr(clientIdAttrKey).set(id);
    clients.put(id, new Client(id, ctx.channel(), multiPaxos, isFromClient,
        threadPool));
    mu.unlock();
  }

  public Client get(Long clientId) {
    mu.lock();
    try {
      return clients.get(clientId);
    } finally {
      mu.unlock();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    var id = ctx.channel().attr(clientIdAttrKey).get();
    mu.lock();
    var it = clients.get(id);
    assert it != null;
    clients.remove(id);
    mu.unlock();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    var clientId = ctx.channel().attr(clientIdAttrKey).get();
    handleRequest(msg, ctx, clientId);
  }

  public void stop() {
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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

  public void handleRequest(String msg, ChannelHandlerContext ctx, long id) {
    if (isFromClient) {
      handleClientRequest(msg, ctx, id);
    } else {
      handlePeerRequest(msg, ctx);
    }
  }

  public void handleClientRequest(String msg, ChannelHandlerContext ctx,
      long id) {
    var command = parse(msg);
    if (command != null) {
      var r = multiPaxos.replicate(command, id);
      if (r.type == MultiPaxosResultType.kOk) {
        ctx.channel().flush();
      } else if (r.type == MultiPaxosResultType.kRetry) {
        ctx.channel().writeAndFlush("retry\n");
      } else {
        assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
        ctx.channel().writeAndFlush("leader is ...\n");
      }
    } else {
      ctx.channel().writeAndFlush("bad command\n");
    }
  }

  public void handlePeerRequest(String msg, ChannelHandlerContext ctx) {
    ObjectMapper mapper = new ObjectMapper();
    Message request = null;
    try {
      request = mapper.readValue(msg, Message.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Message finalRequest = request;
    threadPool.submit(() -> {
      var message = finalRequest.getMsg();
      var responseJson = "";
      Message tcpResponse;
      var tcpResponseJson = "";
      try {
        switch (finalRequest.getType()) {
          case PREPAREREQUEST -> {
            var prepareRequest = mapper.readValue(message,
                PrepareRequest.class);
            var prepareResponse = multiPaxos.prepare(prepareRequest);
            responseJson = mapper.writeValueAsString(prepareResponse);
            tcpResponse = new Message(PREPARERESPONSE,
                finalRequest.getChannelId(),
                responseJson);
            tcpResponseJson = mapper.writeValueAsString(tcpResponse);
          }
          case ACCEPTREQUEST -> {
            var acceptRequest = mapper.readValue(message, AcceptRequest.class);
            var acceptResponse = multiPaxos.accept(acceptRequest);
            responseJson = mapper.writeValueAsString(acceptResponse);
            tcpResponse = new Message(ACCEPTRESPONSE,
                finalRequest.getChannelId(),
                responseJson);
            tcpResponseJson = mapper.writeValueAsString(tcpResponse);
          }
          case COMMITREQUEST -> {
            var commitRequest = mapper.readValue(message, CommitRequest.class);
            var commitResponse = multiPaxos.commit(commitRequest);
            responseJson = mapper.writeValueAsString(commitResponse);
            tcpResponse = new Message(COMMITRESPONSE,
                finalRequest.getChannelId(),
                responseJson);
            tcpResponseJson = mapper.writeValueAsString(tcpResponse);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
      ctx.channel().writeAndFlush(tcpResponseJson + "\n");
    });
  }
}
