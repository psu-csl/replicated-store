package replicantv2;

import ch.qos.logback.classic.Logger;
import command.Command;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;
import paxos.MultiPaxos;
import paxos.MultiPaxosResultType;
import replicant.KVRequest;

@Sharable
public class ClientHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientHandler.class);
  private final Map<Long, Channel> channels = new HashMap<>();
  private final long numPeers;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final long id;
  private final MultiPaxos multiPaxos;
  private long nextClientId;

  public ClientHandler(long id, long numPeers, MultiPaxos multiPaxos) {
    this.nextClientId = this.id = id;
    this.numPeers = numPeers;
    this.multiPaxos = multiPaxos;
  }

  private long nextClientId() {
    var id = nextClientId;
    nextClientId += numPeers;
    return id;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    var id = nextClientId();
    logger.info("client joined " + ctx);
    channels.put(id, ctx.channel());
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    logger.info("raw msg: " + msg);
    KVRequest kvRequest;
    try {
      kvRequest = objectMapper.readValue(msg, KVRequest.class);
    } catch (IOException e) {
      logger.error("couldn't parse msg: " + e.getMessage());
      ctx.channel().writeAndFlush("bad command");
      return;
    }
    logger.info("kvRequest: " + kvRequest);
    var command = new Command(kvRequest.getCommandType(), kvRequest.getKey(), kvRequest.getValue());
    var r = multiPaxos.replicate(command, id);
    if (r.type == MultiPaxosResultType.kRetry) {
      ctx.channel().writeAndFlush("retry");
    } else {
      assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
      ctx.channel().writeAndFlush("leader is ...");
    }
    // TODO: rm this; ycsb waits answer to send next command, verify it...
    ctx.channel().writeAndFlush("ack " + msg);
  }

  public void respond(Long clientId, String value) {
    var client = channels.get(clientId);
    if (client == null) {
      return;
    }
    client.writeAndFlush(value);
  }
}
