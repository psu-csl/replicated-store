package replicantv2;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.net.server.Client;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kvstore.KVStore;
import kvstore.MemKVStore;
import log.Log;
import org.slf4j.LoggerFactory;
import paxos.Configuration;
import paxos.MultiPaxos;

public class Replicant {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(Replicant.class);
  private final ExecutorService executorThread = Executors.newSingleThreadExecutor();
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final long id;
  private final long numPeers;
  private final String ipPort;

  private final MultiPaxos multiPaxos;
  private final KVStore kvStore = new MemKVStore();
  private final Log log = new Log(kvStore);

  private ClientHandler clientHandler;

  public Replicant(Configuration config) {
    this.id = config.getId();
    this.ipPort = config.getPeers().get((int) id);
    this.multiPaxos = new MultiPaxos(log, config);
    this.numPeers = config.getPeers().size();
  }

  public void start() {
    // multiPaxos.start();
    // startExecutorThread();
    startServer();
  }

  public void stop() {
    stopExecutorThread();
    stopServer();
    multiPaxos.stop();
  }

  private void startServer() {

    clientHandler = new ClientHandler(id, numPeers, multiPaxos);

    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(
          new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
              ch.pipeline().addLast(new StringDecoder());
              ch.pipeline().addLast(new StringEncoder());
              ch.pipeline().addLast(clientHandler);
            }
          }).option(ChannelOption.SO_BACKLOG, 5).childOption(ChannelOption.SO_KEEPALIVE, true);

      int port = Integer.parseInt(ipPort.substring(ipPort.indexOf(":") + 1)) + 1;
      logger.info(id + " starting server at port " + port);
      ChannelFuture f = b.bind(port).sync();
      f.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }

  private void stopServer() {
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
  }

  private void startExecutorThread() {
    logger.info(id + " starting executor thread");
    executorThread.submit(this::executorThread);
  }

  private void stopExecutorThread() {
    logger.info(id + " stopping executor thread");
    log.stop();
    executorThread.shutdown();
  }


  private void executorThread() {
    while (true) {
      var r = log.execute();
      if (r == null) {
        break;
      }
      var clientId = r.getKey();
      var result = r.getValue();
      // TODO: respond the result to the client
      clientHandler.respond(clientId, result.getValue());
    }
  }

}
