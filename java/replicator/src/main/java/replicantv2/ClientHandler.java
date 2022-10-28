package replicantv2;

import ch.qos.logback.classic.Logger;
import command.Command;
import command.Command.CommandType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
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
  private AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf("ClientID");

  public ClientHandler(long id, long numPeers, MultiPaxos multiPaxos) {
    this.nextClientId = this.id = id;
    this.numPeers = numPeers;
    this.multiPaxos = multiPaxos;
  }

  public static Command parse(String request) {

    if (request == null) {
      return null;
    }
    String[] tokens = request.split(" ");//"\\s+");
    String command = tokens[0];
    String key = tokens[1];
    Command res = new Command();
    res.setKey(key);
    switch (command) {
      case "get":
        res.setCommandType(CommandType.Get);
        break;
      case "del":
        res.setCommandType(CommandType.Del);
        break;
      case "put":
        res.setCommandType(CommandType.Put);
        String value = tokens[2];
        if (value == null) {
          return null;
        }
        res.setValue(value);
        break;
      default:
        return null;
    }
    return res;
  }

  private long nextClientId() {
    nextClientId += numPeers;
    return nextClientId;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    var id = nextClientId();
    logger.info("client joined " + ctx);
    ctx.channel().attr(clientIdAttrKey).set(id);
    channels.put(id, ctx.channel());
  }


  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    // logger.info("raw msg: " + msg);
    var command = parse(msg);
    var clientId = ctx.channel().attr(clientIdAttrKey).get();
    var r = multiPaxos.replicate(command, clientId);
    if (r.type == MultiPaxosResultType.kOk) {
      ctx.channel().flush();
    } else if (r.type == MultiPaxosResultType.kRetry) {
      ctx.channel().writeAndFlush("retry\n");
    } else {
      assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
      ctx.channel().writeAndFlush("leader is ...\n");
    }
  }

  public void respond(Long clientId, String value) {
    var client = channels.get(clientId);
    if (client == null) {
      return;
    }
    client.writeAndFlush(value + "\n");
  }
}
