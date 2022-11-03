package replicant;

import ch.qos.logback.classic.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kvstore.MemKVStore;
import log.Log;
import org.slf4j.LoggerFactory;
import multipaxos.Configuration;
import multipaxos.MultiPaxos;

public class Replicant {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(Replicant.class);
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final long id;
  private final Log log;
  private final MultiPaxos multiPaxos;
  private final String ipPort;
  private ClientManager clientManager;
  private final ExecutorService executorThread = Executors.newSingleThreadExecutor();


  public Replicant(Configuration config) {
    this.id = config.getId();
    this.log = new Log(new MemKVStore());
    this.multiPaxos = new MultiPaxos(log, config);
    this.ipPort = config.getPeers().get((int) id);
    clientManager = new ClientManager(id, config.getPeers().size(), multiPaxos);

  }

  public void start() {
    multiPaxos.start();
    startExecutorThread();
    startServer();
  }

  public void stop() {
    stopServer();
    stopExecutorThread();
    multiPaxos.stop();
  }

  private void startServer() {
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(
          new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
              ch.pipeline().addLast("framer",
                  new DelimiterBasedFrameDecoder(2048, Delimiters.lineDelimiter()));
              ch.pipeline().addLast("decoder", new StringDecoder());
              ch.pipeline().addLast("encoder", new StringEncoder());
              ch.pipeline().addLast(clientManager);
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
      var id = r.getKey();
      var result = r.getValue();
      var client = clientManager.get(id);
      if(client != null)
        client.write(result.getValue());
    }
  }

}
