package replicant;

import ch.qos.logback.classic.Logger;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import java.util.HashMap;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

@Sharable
public class ClientManager extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientManager.class);
  private final long numPeers;
  private final MultiPaxos multiPaxos;
  private final AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf("ClientID");
  private final HashMap<Long, Client> clients = new HashMap<>();
  private long nextClientId;

  public ClientManager(long id, long numPeers, MultiPaxos multiPaxos) {
    this.nextClientId = id;
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
    ctx.channel().attr(clientIdAttrKey).set(id);
    clients.put(id, new Client(id, ctx.channel(), multiPaxos));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    var id = ctx.channel().attr(clientIdAttrKey).get();
    var it = clients.get(id);
    assert it != null;
    clients.remove(id);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    var clientId = ctx.channel().attr(clientIdAttrKey).get();
    var client = clients.get(clientId);
    if (client != null) {
      client.read(msg);
    }
  }

  public Client get(Long clientId) {
    return clients.get(clientId);
  }
}
