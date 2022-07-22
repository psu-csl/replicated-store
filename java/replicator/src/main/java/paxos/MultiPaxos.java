package paxos;

import static command.Command.CommandType.kDel;
import static command.Command.CommandType.kGet;
import static command.Command.CommandType.kPut;
import static multipaxos.CommandType.DEL;
import static multipaxos.CommandType.GET;
import static multipaxos.CommandType.PUT;

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
import multipaxos.AcceptRequest;
import multipaxos.AcceptResponse;
import multipaxos.Command;
import multipaxos.CommandType;
import multipaxos.HeartbeatRequest;
import multipaxos.HeartbeatResponse;
import multipaxos.Instance;
import multipaxos.InstanceState;
import multipaxos.MultiPaxosRPCGrpc;
import multipaxos.PrepareRequest;
import multipaxos.PrepareResponse;
import multipaxos.ResponseType;
import org.slf4j.LoggerFactory;

class PrepareState {

  public long numRpcs;
  public List<ArrayList<Instance>> responses;
  public ReentrantLock mu;
  public Condition cv;

  public PrepareState() {
    this.numRpcs = 0;
    this.responses = new ArrayList<ArrayList<Instance>>();
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }
}

class HeartbeatState {

  public long numRpcs;
  List<Long> responses;
  ReentrantLock mu;
  Condition cv;

  public HeartbeatState() {
    this.numRpcs = 0;
    this.responses = new ArrayList<>();
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }
}

class RpcPeer {

  public long id;
  public ManagedChannel stub;

  public RpcPeer(long id, ManagedChannel stub) {
    this.id = id;
    this.stub = stub;
  }
}

public class MultiPaxos extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(MultiPaxos.class);
  private final ReentrantLock mu;
  private final Condition cvLeader;
  private final Condition cvFollower;
  private Log log_;
  private Server rpcServer;
  private boolean rpcServerRunning;
  private Condition rpcServerRunningCv;
  private long lastHeartbeat;
  private long heartbeatInterval;
  private long id;
  private long ballot;
  private AtomicBoolean running;
  private ExecutorService heartbeatThread;
  private ExecutorService prepareThread;
  private List<Long> heartbeatResponses;
  private List<RpcPeer> rpcPeers;
  private ExecutorService threadPool;
  private int heartbeatDelta;
  private long prepareNumRpcs;
  private List<List<Instance>> prepareOkResponses;
  private ReentrantLock prepareMu;
  private Condition prepareCv;
  private PrepareRequest.Builder prepareRequestBuilder;

  public MultiPaxos(Log log, Configuration config) {
    this.heartbeatDelta = config.getHeartbeatDelta();
    this.id = config.getId();
    this.ballot = kMaxNumPeers;
    this.heartbeatInterval = config.getHeartbeatPause();
    this.log_ = log;
    this.running = new AtomicBoolean(false);
    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    cvFollower = mu.newCondition();
    heartbeatThread = Executors.newSingleThreadExecutor();
    prepareThread = Executors.newSingleThreadExecutor();
    lastHeartbeat = 0;
    heartbeatResponses = new ArrayList<>();
    rpcServerRunning = false;

    prepareOkResponses = new ArrayList<>();
    prepareMu = new ReentrantLock();
    prepareCv = prepareMu.newCondition();
    prepareRequestBuilder = PrepareRequest.newBuilder();

    rpcServerRunningCv = mu.newCondition();

    threadPool = Executors.newFixedThreadPool(config.getThreadPoolSize());
    rpcPeers = new ArrayList<>();
    long rpcId = 0;
    for (var peer : config.getPeers()) {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(peer).usePlaintext().build();
      rpcPeers.add(new RpcPeer(rpcId++, channel));
    }
    rpcServer = ServerBuilder.forPort(config.getPort()).addService(this).build();
  }

  public static Command makeProtoCommand(command.Command command) {
    CommandType commandType = null;
    switch (command.getCommandType()) {
      case kDel -> commandType = DEL;
      case kGet -> commandType = GET;
      case kPut -> commandType = PUT;
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

  public static command.Command makeCommand(Command cmd) {
    command.Command command = new command.Command();
    command.Command.CommandType commandType = null;
    switch (cmd.getType()) {
      case DEL -> commandType = kDel;
      case GET -> commandType = kGet;
      case PUT -> commandType = kPut;
      case UNRECOGNIZED -> {
      }
    }
    command.setCommandType(commandType);
    command.setKey(cmd.getKey());
    command.setValue(cmd.getValue());
    return command;
  }

  public static log.Instance makeInstance(Instance inst) {
    log.Instance instance = new log.Instance();
    instance.setBallot(inst.getBallot());
    instance.setIndex(inst.getIndex());
    instance.setClientId(inst.getClientId());
    instance.setCommand(makeCommand(inst.getCommand()));
    return instance;
  }

  public long nextBallot() {
    mu.lock();
    try {
      var oldBallot = ballot;
      ballot += kRoundIncrement;
      ballot = (ballot & ~kIdBits) | id;
      logger.info(id + " became a leader: ballot: " + oldBallot + " -> " + ballot);
      cvLeader.signal();
      return ballot;
    } finally {
      mu.unlock();
    }
  }

  public void setBallot(long ballot) {
    var oldId = this.ballot & kIdBits;
    var newId = this.ballot & kIdBits;
    if ((oldId == id || oldId == kMaxNumPeers) && oldId != newId) {
      logger.info(id + " became a follower: ballot: " + this.ballot + " -> " + ballot);
      cvFollower.signal();
    }
    this.ballot = ballot;
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
    assert (rpcServer != null);
    try {
      rpcServer.start();
      mu.lock();
      rpcServerRunning = true;
      rpcServerRunningCv.signal();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      mu.unlock();
    }
    // System.out.println("Server started listening on port: " + server.getPort());
    // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    // shutdown();
    // System.out.println("Server is successfully shut down");
    // }));
    logger.info(id + " starting rpc server at " + rpcServer.getPort());
    blockUntilShutDown();
  }

  private void blockUntilShutDown() {
    if (rpcServer != null) {
      try {
        rpcServer.awaitTermination();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void stop() {
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
    //assert (rpcServer != null);
    try {
      mu.lock();
      while (!rpcServerRunning) {
        rpcServerRunningCv.await();
      }
      mu.unlock();
      logger.info(id + " stopping rpc at " + rpcServer.getPort());
      rpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      for (var peer : rpcPeers) {
        peer.stub.shutdown();
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
        var res = sendHeartbeats(globalLastExecuted);
        if (res != null) {
          globalLastExecuted = res;
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

  public Long sendHeartbeats(long globalLastExecuted) {
    var state = new HeartbeatState();
    HeartbeatRequest.Builder request = HeartbeatRequest.newBuilder();
    mu.lock();
    request.setBallot(ballot);
    mu.unlock();
    request.setSender(this.id);
    request.setLastExecuted(log_.getLastExecuted());
    request.setGlobalLastExecuted(globalLastExecuted);
    for (var peer : rpcPeers) {
      threadPool.submit(() -> {

        var response = MultiPaxosRPCGrpc.newBlockingStub(peer.stub)
            .heartbeat(request.build());
        logger.info(id + " sent heartbeat to " + peer.id);
        state.mu.lock();
        ++state.numRpcs;
        state.responses.add((long) response.getLastExecuted());
        state.mu.unlock();
        state.cv.signal();
      });
    }
    state.mu.lock();
    while (isLeader() && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (state.responses.size() == rpcPeers.size()) {
      return heartbeatResponses.stream().mapToLong(v -> v).min()
          .orElseThrow(NoSuchElementException::new);
    }
    return null;
  }

  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest,
      StreamObserver<HeartbeatResponse> responseObserver) {
    logger.info(id + " received heartbeat rpc from " + heartbeatRequest.getSender());
    mu.lock();
    try {
      if (heartbeatRequest.getBallot() >= ballot) {
        lastHeartbeat = Instant.now().toEpochMilli();
        setBallot(heartbeatRequest.getBallot());
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

  public List<ArrayList<Instance>> sendPrepares() {
    var state = new PrepareState();
    PrepareRequest.Builder request = PrepareRequest.newBuilder();
    request.setSender(this.id);
    request.setBallot(nextBallot());
    for (var peer : rpcPeers) {
      threadPool.submit(() -> {
        var response = MultiPaxosRPCGrpc.newBlockingStub(peer.stub)
            .prepare(request.build());
        logger.info(id + " sent prepare request to " + peer.id);
        state.mu.lock();
        ++state.numRpcs;
        if (response.getType() == ResponseType.OK) {
          ArrayList<Instance> tempLog = new ArrayList<>(response.getInstancesList());
          state.responses.add(tempLog);
        } else {
          assert (response.getType() == ResponseType.REJECT);
          mu.lock();
          if (response.getBallot() >= ballot) {
            setBallot(response.getBallot());
          }
          mu.unlock();
        }
        state.cv.signal();
        state.mu.unlock();
      });
    }
    state.mu.lock();
    while (isLeader() && state.responses.size() <= rpcPeers.size() / 2
        && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (state.responses.size() > rpcPeers.size() / 2) {
      return state.responses;
    }
    state.mu.unlock();

    return null;
  }

  @Override
  public void prepare(PrepareRequest request, StreamObserver<PrepareResponse> responseObserver) {
    logger.info(id + " received prepare rpc from " + request.getSender());
    mu.lock();
    PrepareResponse.Builder responseBuilder = PrepareResponse.newBuilder();
    try {
      if (request.getBallot() >= ballot) {
        setBallot(request.getBallot());

        for (var i : log_.instancesSinceGlobalLastExecuted()) {
          responseBuilder.addInstances(makeProtoInstance(i));
        }
        responseBuilder = responseBuilder.setType(ResponseType.OK);
      } else {
        responseBuilder = responseBuilder.setBallot(ballot).setType(ResponseType.REJECT);
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
      while (running.get() && !isLeader()) {
        var sleepTime = heartbeatInterval + random.nextInt(heartbeatDelta,
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

        var res = sendPrepares();
        if (res != null) {
          continue;
        }

        mu.lock();
        if (isLeaderLockless()) {
          logger.info(id + " leader and ready to serve");
        }
        mu.unlock();

      }
    }
    logger.info(id + " stopping prepare thread");
  }

  @Override
  public void accept(AcceptRequest request, StreamObserver<AcceptResponse> responseObserver) {
    logger.info(this.id + " received rpc from " + request.getSender());
    mu.lock();
    AcceptResponse.Builder responseBuilder = AcceptResponse.newBuilder();
    try {
      if (request.getInstance().getBallot() >= this.ballot) {
        setBallot(request.getInstance().getBallot());
        log_.append(makeInstance(request.getInstance()));
        responseBuilder = responseBuilder.setType(ResponseType.OK);
      } else {
        responseBuilder = responseBuilder.setBallot(ballot).setType(ResponseType.REJECT);
      }
      var response = responseBuilder.build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

}
