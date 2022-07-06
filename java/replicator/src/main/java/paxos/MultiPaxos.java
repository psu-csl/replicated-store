package paxos;

import ch.qos.logback.classic.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatRequest.Builder;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;
import org.slf4j.LoggerFactory;

public class MultiPaxos extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(MultiPaxos.class);
  private final ReentrantLock mu;

  private final Condition cvLeader;
  private Log log;
  private Server server;
  private Instant lastHeartbeat;
  private long heartbeatPause;
  private long id;
  private long ballot;
  private AtomicBoolean running;
  private ExecutorService heartbeatThread;
  private long heartbeatNumResponses;
  private List<Long> heartbeatOkResponses;
  private ReentrantLock heartbeatMu;
  private Condition heartbeatCv;
  private Builder heartbeatRequestBuilder;
  private List<ManagedChannel> rpcPeers;
  private ExecutorService tp;


  public MultiPaxos(Log log, Configuration config) {
    this.id = config.getId();
    this.ballot = kMaxNumPeers;
    this.heartbeatPause = config.getHeartbeatPause();
    this.log = log;
    this.running = new AtomicBoolean(false);
    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    heartbeatThread = Executors.newSingleThreadExecutor();

    heartbeatNumResponses = 0;
    heartbeatOkResponses = new ArrayList<>();
    heartbeatMu = new ReentrantLock();
    heartbeatCv = heartbeatMu.newCondition();

    heartbeatRequestBuilder = HeartbeatRequest.newBuilder();
    tp = Executors.newFixedThreadPool(config.getThreadPoolSize());
    rpcPeers = new ArrayList<>();
    for (var peer : config.getPeers()) {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(peer).usePlaintext().build();
      rpcPeers.add(channel);
    }
    server = ServerBuilder.forPort(config.getPort()).addService(this).build();
  }

  public long nextBallot() {
    mu.lock();
    try {
      ballot += kRoundIncrement;
      ballot = (ballot & ~kIdBits) | id;
      cvLeader.signalAll();
      return ballot;
    } finally {
      mu.unlock();
    }
  }

  public long leader() {
    mu.lock();
    try {
      return ballot & kIdBits;
    } finally {
      mu.unlock();
    }
  }

  public boolean isLeader() {
    mu.lock();
    try {
      return isLeaderLockless();
    } finally {
      mu.unlock();
    }
  }

  public boolean isLeaderLockless() {
    return (ballot & kIdBits) == id;
  }

  public boolean isSomeoneElseLeader() {
    mu.lock();
    try {
      var id = ballot & kIdBits;
      return id != this.id && id < kMaxNumPeers;
    } finally {
      mu.unlock();
    }
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void start() {
    assert (!running.get());
    running.set(true);
    // heartbeatThread.submit()
    heartbeatThread.submit(this::heartbeatThread);
    assert (server != null);
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // System.out.println("Server started listening on port: " + server.getPort());
    // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    // shutdown();
    // System.out.println("Server is successfully shut down");
    // }));
    logger.info(id + " starting rpc server at " + server.getPort());
    blockUntilShutDown();
  }

  private void blockUntilShutDown() {
    if (server != null) {
      try {
        server.awaitTermination();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void shutdown() {
    assert (running.get());
    running.set(false);
    mu.lock();
    cvLeader.signalAll();
    mu.unlock();
    // joint heartbeat thread here
    heartbeatThread.shutdown();
    try {
      heartbeatThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assert (server != null);
    try {
      logger.info(id + " stopping rpc at " + server.getPort());
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      for (var peer : rpcPeers) {
        peer.shutdown();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void heartbeatThread() {
    logger.info(id + " starting heartbeat thread");
    while (running.get()) {
      mu.lock();
      while (running.get() && !isLeaderLockless()) {
        try {
          cvLeader.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      mu.unlock();
      var globalLastExecuted = log.getGlobalLastExecuted();
      while (running.get()) {
        heartbeatNumResponses = 0;
        heartbeatOkResponses.clear();

        mu.lock();
        heartbeatRequestBuilder.setBallot(ballot);
        mu.unlock();

        heartbeatRequestBuilder.setLastExecuted(log.getLastExecuted());
        heartbeatRequestBuilder.setGlobalLastExecuted(globalLastExecuted);

        for (var peer : rpcPeers) {
          tp.submit(() -> {

            var response = MultiPaxosRPCGrpc.newBlockingStub(peer)
                .heartbeat(heartbeatRequestBuilder.build());
            logger.info(id + " sent heartbeat to " + peer);
            heartbeatMu.lock();
            ++heartbeatNumResponses;
            heartbeatOkResponses.add(response.getLastExecuted());

            heartbeatMu.unlock();
            heartbeatCv.signal();
          });
        }
        heartbeatMu.lock();
        while (isLeader() && heartbeatNumResponses != rpcPeers.size()) {
          try {
            heartbeatCv.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        if (heartbeatOkResponses.size() == rpcPeers.size()) {
          globalLastExecuted = heartbeatOkResponses.stream().mapToLong(v -> v).min()
              .orElseThrow(NoSuchElementException::new);
        }
        try {
          Thread.sleep(heartbeatPause);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (!isLeader()) {
          break;
        }
      }
      logger.info(id + " stopping heartbeat thread");
    }
  }

  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest,
      StreamObserver<HeartbeatResponse> responseObserver) {
    logger.info(id + " received heartbeat rpc");
    mu.lock();
    try {
      if (heartbeatRequest.getBallot() >= ballot) {
        lastHeartbeat = Instant.now();
        ballot = heartbeatRequest.getBallot();

        log.commitUntil(heartbeatRequest.getLastExecuted(), heartbeatRequest.getBallot());
        log.trimUntil(heartbeatRequest.getGlobalLastExecuted());

      }
      HeartbeatResponse response = HeartbeatResponse.newBuilder()
          .setLastExecuted(log.getLastExecuted()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

}
