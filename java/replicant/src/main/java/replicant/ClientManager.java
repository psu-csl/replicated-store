package replicant;

import ch.qos.logback.classic.Logger;
import command.Command;
import command.Command.CommandType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;

@Sharable
public class ClientManager extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientManager.class);
  private final ConcurrentHashMap<Long, Client> clients = new ConcurrentHashMap<>();
  private final long numPeers;
  private final MultiPaxos multiPaxos;
  private long nextClientId;
  private final AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf("ClientID");

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
    clients.put(id, new Client(id,ctx.channel(),multiPaxos));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    var id = ctx.channel().attr(clientIdAttrKey).get();
    var it = clients.get(id);
    assert it!=null;
    clients.remove(id);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    var clientId = ctx.channel().attr(clientIdAttrKey).get();
    var client = clients.get(clientId);
    if(client!=null)
      client.read(msg);

  }


  public Client get(Long clientId){
    return clients.get(clientId);
  }
}
