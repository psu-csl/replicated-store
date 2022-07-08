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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxos.Command;
import multipaxos.CommandType;
import multipaxos.HeartbeatRequest;
import multipaxos.HeartbeatRequest.Builder;
import multipaxos.HeartbeatResponse;
import multipaxos.Instance;
import multipaxos.InstanceState;
import multipaxos.MultiPaxosRPCGrpc;
import multipaxos.PrepareRequest;
import multipaxos.PrepareResponse;
import multipaxos.ResponseType;
import org.slf4j.LoggerFactory;

public class MultiPaxos extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(MultiPaxos.class);
  private final ReentrantLock mu;
  private final Condition cvLeader;
  private final Condition cvFollower;
  private Log log_;
  private Server server;
  private long lastHeartbeat;
  private long heartbeatInterval;
  private long id;
  private long ballot;
  private AtomicBoolean running;
  private ExecutorService heartbeatThread;
  private ExecutorService prepareThread;
  private long heartbeatNumResponses;
  private List<Long> heartbeatOkResponses;
  private ReentrantLock heartbeatMu;
  private Condition heartbeatCv;
  private Builder heartbeatRequestBuilder;
  private List<ManagedChannel> rpcPeers;
  private ExecutorService threadPool;
  private int offset;
  private boolean ready;

  public MultiPaxos(Log log, Configuration config) {
    this.offset = config.getOffset();
    this.id = config.getId();
    this.ballot = kMaxNumPeers;
    this.heartbeatInterval = config.getHeartbeatPause();
    this.log_ = log;
    this.running = new AtomicBoolean(false);
    ready = false;
    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    cvFollower = mu.newCondition();
    heartbeatThread = Executors.newSingleThreadExecutor();
    prepareThread = Executors.newSingleThreadExecutor();
    heartbeatNumResponses = 0;
    heartbeatOkResponses = new ArrayList<>();
    heartbeatMu = new ReentrantLock();
    heartbeatCv = heartbeatMu.newCondition();

    heartbeatRequestBuilder = HeartbeatRequest.newBuilder();
    threadPool = Executors.newFixedThreadPool(config.getThreadPoolSize());
    rpcPeers = new ArrayList<>();
    for (var peer : config.getPeers()) {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(peer).usePlaintext().build();
      rpcPeers.add(channel);
    }
    server = ServerBuilder.forPort(config.getPort()).addService(this).build();
  }

  public static Command makeProtoCommand(command.Command command) {
    CommandType commandType = null;
    switch (command.getCommandType()) {
      case kDel -> commandType = CommandType.DEL;
      case kGet -> commandType = CommandType.GET;
      case kPut -> commandType = CommandType.PUT;
    }
    return Command.newBuilder().setType(commandType).setKey(command.getKey())
        .setValue(command.getValue()).build();
  }

  public static Instance makeProtoInstance(log.Instance inst) {
    InstanceState state = null;
    switch (inst.getState()) {
      case kInProgress -> state = InstanceState.INPROGRESS;
      case kCommitted -> state = InstanceState.COMMITTED;
      case kExecuted -> state = InstanceState.EXECUTED;
    }

    return Instance.newBuilder().setBallot(inst.getBallot()).setIndex(inst.getIndex())
        .setClientId(inst.getClientId()).setState(state)
        .setCommand(makeProtoCommand(inst.getCommand())).build();
  }

  public long nextBallot() {
    mu.lock();
    try {
      ballot += kRoundIncrement;
      ballot = (ballot & ~kIdBits) | id;
      ready = false;
      cvLeader.signal();
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

  public void waitUntilLeader() {
    mu.lock();
    try {
      while (running.get() && !isLeaderLockless()) {
        cvLeader.await();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      mu.unlock();
    }
  }

  public void waitUntilFollower() {
    mu.lock();
    try {
      while (running.get() && isLeaderLockless()) {
        cvFollower.await();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
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
    prepareThread.submit(this::prepareThread);
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
    cvLeader.signal();
    mu.unlock();
    // joint heartbeat thread here
    heartbeatThread.shutdown();
    try {
      heartbeatThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    mu.lock();
    cvFollower.signal();
    mu.unlock();
    prepareThread.shutdown();
    try {
      prepareThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
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
      waitUntilLeader();
      var globalLastExecuted = log_.getGlobalLastExecuted();
      while (running.get() && isLeader()) {
        heartbeatNumResponses = 0;
        heartbeatOkResponses.clear();

        mu.lock();
        heartbeatRequestBuilder.setBallot(ballot);
        mu.unlock();

        heartbeatRequestBuilder.setLastExecuted(log_.getLastExecuted());
        heartbeatRequestBuilder.setGlobalLastExecuted(globalLastExecuted);

        for (var peer : rpcPeers) {
          threadPool.submit(() -> {

            var response = MultiPaxosRPCGrpc.newBlockingStub(peer)
                .heartbeat(heartbeatRequestBuilder.build());
            logger.info(id + " sent heartbeat to " + peer);
            heartbeatMu.lock();
            ++heartbeatNumResponses;
            heartbeatOkResponses.add((long) response.getLastExecuted());

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
          Thread.sleep(heartbeatInterval);
        } catch (InterruptedException e) {
          e.printStackTrace();
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
        lastHeartbeat = Instant.now().toEpochMilli();
        ballot = heartbeatRequest.getBallot();

        log_.commitUntil(heartbeatRequest.getLastExecuted(), heartbeatRequest.getBallot());
        log_.trimUntil(heartbeatRequest.getGlobalLastExecuted());

      }
      HeartbeatResponse response = HeartbeatResponse.newBuilder()
          .setLastExecuted((int) log_.getLastExecuted()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

  @Override
  public void prepare(PrepareRequest request, StreamObserver<PrepareResponse> responseObserver) {
    logger.info(id + " received prepare rpc");
    mu.lock();
    PrepareResponse.Builder responseBuilder = PrepareResponse.newBuilder();
    try {
      if (request.getBallot() >= ballot) {
        ballot = request.getBallot();
        responseBuilder.setType(ResponseType.OK);
        for (var i : log_.instancesForPrepare()) {
          responseBuilder.addInstances(makeProtoInstance(i));
        }
      } else {
        responseBuilder.setType(ResponseType.REJECT);
      }
      var response = responseBuilder.build();
      logger.info("sending response : " + response);
      responseObserver.onNext(response);
      responseObserver.onCompleted();

    } finally {
      mu.unlock();
    }
  }

  void prepareThread() {
    Random random = new Random();
    logger.info(id + " starting prepare thread");
    while (running.get()) {
      waitUntilLeader();
      while (running.get()) {
        var sleepTime = random.nextInt(offset,
            (int) heartbeatInterval); // check whether to add with heartbeatInterval or not
        logger.info(id + " prepare thread sleeping for " + sleepTime);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info(id + " prepare thread woke up");
        var now = Instant.now().toEpochMilli();
        if (now - lastHeartbeat < heartbeatInterval) {
          continue;
        }
      }
    }
    logger.info(id + " stopping prepare thread");
  }

}
