package paxos;

import static command.Command.CommandType.kDel;
import static command.Command.CommandType.kGet;
import static command.Command.CommandType.kPut;
import static log.Log.insert;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

enum MultiPaxosResultType{
  kOk,
  kRetry,
  kSomeoneElseLeader
}

class MultiPaxosResult{
  public MultiPaxosResultType type;
  public long leader;

  public MultiPaxosResult(MultiPaxosResultType type, Long leader){
    this.type = type;
    this.leader = leader;
  }
}
class AcceptState {

  public long numRpcs;
  public long numOks;
  public ReentrantLock mu;
  public Condition cv;

  public AcceptState() {
    this.numRpcs = 0;
    this.numOks = 0;
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }

}

class PrepareState {

  public long numRpcs;
  public long numOks;
  public HashMap<Long, log.Instance> log;
  public ReentrantLock mu;
  public Condition cv;

  public PrepareState() {
    this.numRpcs = 0;
    this.numOks = 0;
    this.log = new HashMap<>();
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }
}

class HeartbeatState {

  public long numRpcs;
  public long numOks;
  public long minLastExecuted;
  public ReentrantLock mu;
  public Condition cv;

  public HeartbeatState() {
    this.numRpcs = 0;
    this.numOks = 0;
    this.minLastExecuted = 0;
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
  private final Log log_;
  private final Server rpcServer;
  private final Condition rpcServerRunningCv;
  private final long heartbeatInterval;
  private final AtomicBoolean running;
  private final ExecutorService heartbeatThread;
  private final ExecutorService prepareThread;
  private final List<Long> heartbeatResponses;
  private final List<RpcPeer> rpcPeers;
  private final ExecutorService threadPool;
  private final int heartbeatDelta;
  private boolean ready;
  private boolean rpcServerRunning;
  private long lastHeartbeat;
  private long id;
  private long ballot;

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
    ready = false;

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

  public long leaderLockless(){
    return ballot & kIdBits;
  }

  public long nextBallot() {
    mu.lock();
    try {
      var oldBallot = ballot;
      ballot += kRoundIncrement;
      ballot = (ballot & ~kIdBits) | id;
      ready = false;
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
      return leaderLockless();
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
      return isSomeoneElseLeaderLockless();
    } finally {
      mu.unlock();
    }
  }

  public boolean isSomeoneElseLeaderLockless(){
    var id = ballot & kIdBits;
    return id!=this.id && id<kMaxNumPeers;
  }

  public boolean isLeaderAndReady(){
    mu.lock();
    try{
      return isLeaderLockless() && ready;
    }finally {
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
      var gle = log_.getGlobalLastExecuted();
      while (running.get() && isLeader()) {
        gle = sendHeartbeats(gle);
        sleepForHeartbeatInterval();
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
    request.setLastExecuted(state.minLastExecuted);
    request.setGlobalLastExecuted(globalLastExecuted);
    for (var peer : rpcPeers) {
      threadPool.submit(() -> {

        var response = MultiPaxosRPCGrpc.newBlockingStub(peer.stub)
            .heartbeat(request.build());
        logger.info(id + " sent heartbeat to " + peer.id);
        state.mu.lock();
        ++state.numRpcs;
        ++state.numOks;
        if (response.getLastExecuted() < state.minLastExecuted) {
          state.minLastExecuted = response.getLastExecuted();
        }
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
    if (state.numOks == rpcPeers.size()) {
      state.mu.unlock();
      return state.minLastExecuted;
    }
    state.mu.unlock();
    return globalLastExecuted;
  }


  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest,
      StreamObserver<HeartbeatResponse> responseObserver) {
    logger.info(id + " received heartbeat rpc from " + heartbeatRequest.getSender());
    mu.lock();
    try {
      if (heartbeatRequest.getBallot() >= ballot) {
        lastHeartbeat = Now();
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

  public HashMap<Long, log.Instance> sendPrepares() {
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
          ++state.numOks;
          for (int i = 0; i < response.getInstancesCount(); ++i) {
            insert(state.log, makeInstance(response.getInstances(i)));
          }
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
    while (isLeader() && state.numOks <= rpcPeers.size() / 2
        && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (state.numOks > rpcPeers.size() / 2) {
      state.mu.unlock();
      return state.log;
    }
    state.mu.unlock(); // TODO: verify moving this above if and deleting unlock inside if

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
      waitUntilFollower();
      while (running.get() && !isLeader()) {
        sleepForRandomInterval();
        if (receivedHeartbeat()) {
          continue;
        }

        var logs = sendPrepares();
        if (logs != null) {
          replay(logs);
        }
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

  public MultiPaxosResult sendAccepts(command.Command command, long index, long clientId) {
    var state = new AcceptState();
    log.Instance instance = new log.Instance();
    mu.lock();
    instance.setBallot(this.ballot);
    mu.unlock();
    instance.setIndex(index);
    instance.setClientId(clientId);
    instance.setState(log.Instance.InstanceState.kInProgress);
    instance.setCommand(command);

    AcceptRequest.Builder request = AcceptRequest.newBuilder();
    request.setSender(this.id);
    request.setInstance(makeProtoInstance(instance));

    for (var peer : rpcPeers) {
      threadPool.submit(() -> {
        var response = MultiPaxosRPCGrpc.newBlockingStub(peer.stub)
            .accept(request.build());
        logger.info(id + " sent accept request to " + peer.id);
        state.mu.lock();
        ++state.numRpcs;
        if (response.getType() == ResponseType.OK) {
          ++state.numOks;
        } else {
          mu.lock();
          if (response.getBallot() >= this.ballot) {
            setBallot(response.getBallot());
          }
          mu.unlock();
        }
        state.cv.signal();
        state.mu.unlock();
      });
    }
    state.mu.lock();
    while (isLeader() && state.numOks <= rpcPeers.size() / 2
        && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (state.numOks > rpcPeers.size() / 2) {
      state.mu.unlock();
      return new MultiPaxosResult(MultiPaxosResultType.kOk, null);
    }
    state.mu.unlock(); // TODO: verify whether it's safe to unlock before if
    mu.lock();
    if(!isLeaderLockless()) {
      mu.unlock();
      return new MultiPaxosResult(MultiPaxosResultType.kSomeoneElseLeader, leaderLockless());
    }
    mu.unlock();
    return  new MultiPaxosResult(MultiPaxosResultType.kRetry, null);
  }

  public void sleepForHeartbeatInterval() {
    try {
      Thread.sleep(heartbeatInterval);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void sleepForRandomInterval() {
    Random random = new Random();
    var sleepTime = heartbeatInterval + random.nextInt(heartbeatDelta, (int) heartbeatInterval);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public long Now() {
    return Instant.now().toEpochMilli();
  }

  public boolean receivedHeartbeat() {
    var now = Instant.now().toEpochMilli();
    return now - lastHeartbeat < heartbeatInterval;
  }

  public void replay(HashMap<Long, log.Instance> logs) {
    HashMap<Long, log.Instance> mergedLog = new HashMap<>();
    for (Map.Entry<Long, log.Instance> entry : logs.entrySet()) {
      insert(mergedLog, entry.getValue());
    }

    for (Map.Entry<Long, log.Instance> entry : mergedLog.entrySet()) {
      var res = sendAccepts(entry.getValue().getCommand(), entry.getValue().getIndex(),
          entry.getValue().getClientId());
      if(res.type != MultiPaxosResultType.kOk) {
        break;
      }
    }

    mu.lock();
    if (isLeaderLockless()) {
      ready = true;
      logger.info(this.id + " leader is ready to server");
    }
  }

  public MultiPaxosResult replicate(command.Command command, long clientId){
    if(isLeaderAndReady()){
      return sendAccepts(command,log_.advanceLastIndex(),clientId);
    }
    mu.lock();
    try{
      if (isSomeoneElseLeaderLockless()) {
        return new MultiPaxosResult(MultiPaxosResultType.kSomeoneElseLeader, leaderLockless());
      }
      return new MultiPaxosResult(MultiPaxosResultType.kRetry, null);
    }finally {
      mu.unlock();
    }
  }

}

