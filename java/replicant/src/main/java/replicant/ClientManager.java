package replicant;

import ch.qos.logback.classic.Logger;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

@Sharable
public class ClientManager extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientManager.class);
  private long nextClientId;
  private final long numPeers;
  private final MultiPaxos multiPaxos;
  private final AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf("ClientID");

  private final ReentrantLock mu = new ReentrantLock();
  private final HashMap<Long, Client> clients = new HashMap<>();

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
    mu.lock();
    clients.put(id, new Client(id,ctx.channel(),multiPaxos));
    mu.unlock();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    var id = ctx.channel().attr(clientIdAttrKey).get();
    mu.lock();
    var it = clients.get(id);
    assert it!=null;
    clients.remove(id);
    mu.unlock();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String msg) {
    var clientId = ctx.channel().attr(clientIdAttrKey).get();
    mu.lock();
    var client = clients.get(clientId);
    mu.unlock();
    if(client!=null)
      client.read(msg);
  }
  public Client get(Long clientId){
    mu.lock();
    try {
      return clients.get(clientId);
    }finally {
      mu.unlock();
    }
  }
}
