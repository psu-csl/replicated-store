package multipaxos;

import static command.Command.CommandType.Del;
import static command.Command.CommandType.Get;
import static command.Command.CommandType.Put;
import static java.lang.Math.max;
import static log.Log.insert;
import static multipaxos.CommandType.DEL;
import static multipaxos.CommandType.GET;
import static multipaxos.CommandType.PUT;

import ch.qos.logback.classic.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
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
import org.slf4j.LoggerFactory;
import log.Instance;

class BallotResult {

  public boolean isReady;
  public long ballot;

  public BallotResult(long ballot, boolean isReady) {
    this.isReady = isReady;
    this.ballot = ballot;
  }
}

class AcceptState {

  public long numRpcs;
  public long numOks;
  public long leader;
  public ReentrantLock mu;
  public Condition cv;

  public AcceptState(long leader) {
    this.numRpcs = 0;
    this.numOks = 0;
    this.leader = leader;
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }

}

class PrepareState {

  public long numRpcs;
  public long numOks;
  public long leader;
  public long maxLastIndex;
  public HashMap<Long, log.Instance> log;
  public ReentrantLock mu;
  public Condition cv;


  public PrepareState(long leader) {
    this.numRpcs = 0;
    this.numOks = 0;
    this.leader = leader;
    this.maxLastIndex = 0;
    this.log = new HashMap<>();
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
  }
}

class CommitState {

  public long numRpcs;
  public long numOks;
  public long minLastExecuted;
  public long leader;
  public ReentrantLock mu;
  public Condition cv;

  public CommitState(long leader, long minLastExecuted) {
    this.numRpcs = 0;
    this.leader = leader;
    this.numOks = 0;
    this.minLastExecuted = minLastExecuted;
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

public class MultiPaxos extends multipaxos.MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(MultiPaxos.class);
  private long ballot;
  private final Log log;
  private final long id;
  private final AtomicBoolean commitReceived;
  private final long commitInterval;
  private final int port;
  private final List<RpcPeer> rpcPeers;
  private final ReentrantLock mu;
  private final ExecutorService threadPool;

  private final Condition cvLeader;
  private final Condition cvFollower;

  private Server rpcServer;
  private boolean rpcServerRunning;
  private final Condition rpcServerRunningCv;
  private final ExecutorService rpcServerThread;

  private final AtomicBoolean prepareThreadRunning;
  private final ExecutorService prepareThread;

  private final AtomicBoolean commitThreadRunning;
  private final ExecutorService commitThread;

  public MultiPaxos(Log log, Configuration config) {
    this.ballot = kMaxNumPeers;
    this.log = log;
    this.id = config.getId();
    commitReceived = new AtomicBoolean(false);
    this.commitInterval = config.getCommitInterval();

    String target = config.getPeers().get((int) this.id);
    this.port =  Integer.parseInt(target.substring(target.indexOf(":") + 1));

    threadPool = Executors.newFixedThreadPool(config.getThreadPoolSize());
    rpcServerRunning = false;
    prepareThreadRunning = new AtomicBoolean(false);
    commitThreadRunning = new AtomicBoolean(false);

    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    cvFollower = mu.newCondition();
    prepareThread = Executors.newSingleThreadExecutor();
    commitThread = Executors.newSingleThreadExecutor();
    rpcServerThread = Executors.newSingleThreadExecutor();
    rpcServerRunningCv = mu.newCondition();

    rpcPeers = new ArrayList<>();
    long rpcId = 0;
    for (var peer : config.getPeers()) {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(peer).usePlaintext().build();
      rpcPeers.add(new RpcPeer(rpcId++, channel));
    }
  }
  public void start() {
    startPrepareThread();
    startCommitThread();
    startRPCServer();
  }
  public void stop() {
    stopRPCServer();
    stopPrepareThread();
    stopCommitThread();

    threadPool.shutdown();
    try {
      threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void startRPCServer() {
    rpcServer = ServerBuilder.forPort(port).addService(this).build();
    try {
      rpcServer.start();
      logger.info(id + " starting rpc server at " + rpcServer.getPort());
      mu.lock();
      rpcServerRunning = true;
      rpcServerRunningCv.signal();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      mu.unlock();
    }
    rpcServerThread.submit(this::blockUntilShutDown);
  }
  public void stopRPCServer() {
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
    rpcServerThread.shutdown();
  }
  public void startPrepareThread() {
    logger.info(id + " starting prepare thread");
    assert (!prepareThreadRunning.get());
    prepareThreadRunning.set(true);
    prepareThread.submit(this::prepareThread);
  }
  public void stopPrepareThread() {
    logger.info(id + " stopping prepare thread");
    assert (prepareThreadRunning.get());
    mu.lock();
    prepareThreadRunning.set(false);
    cvFollower.signal();
    mu.unlock();
    prepareThread.shutdown();
    try {
      prepareThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  public void startCommitThread() {
    logger.info(id + " starting commit thread");
    assert (!commitThreadRunning.get());
    commitThreadRunning.set(true);
    commitThread.submit(this::commitThread);
  }

  public void stopCommitThread() {
    logger.info(id + " stopping commit thread");
    assert (commitThreadRunning.get());
    mu.lock();
    commitThreadRunning.set(false);
    cvLeader.signal();
    mu.unlock();
    commitThread.shutdown();
    try {
      commitThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  public Result replicate(command.Command command, long clientId) {
    var ballot = ballot();
    if (isLeader(ballot, this.id)) {
        return runAcceptPhase(ballot, log.advanceLastIndex(), command, clientId);

    }
    if (isSomeoneElseLeader(ballot, this.id)) {
      return new Result(MultiPaxosResultType.kSomeoneElseLeader, extractLeaderId(ballot));
    }
    return new Result(MultiPaxosResultType.kRetry, null);
  }
  void prepareThread() {
    while (prepareThreadRunning.get()) {
      mu.lock();
      try {
        while (prepareThreadRunning.get() && isLeader(this.ballot, this.id)) {
          cvFollower.await();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        mu.unlock();
      }
      while (prepareThreadRunning.get()) {
        sleepForRandomInterval();
        if (receivedCommit()) {
          continue;
        }
        var nextBallot = nextBallot();
        var r = runPreparePhase(nextBallot);
        if (r != null) {
          var maxLastIndex = r.getKey();
          var log = r.getValue();
          becomeLeader(nextBallot,maxLastIndex);
          replay(nextBallot, log);
          break;
        }
      }
    }
  }
  public void commitThread() {
    while (commitThreadRunning.get()) {
      mu.lock();
      try {
        while (commitThreadRunning.get() && !isLeader(this.ballot, this.id)) {
          cvLeader.await();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        mu.unlock();
      }
      var gle = log.getGlobalLastExecuted();
      while (commitThreadRunning.get()) {
        var ballot = ballot();
        if (!isLeader(ballot, this.id)) {
          break;
        }
        gle = runCommitPhase(ballot, gle);
        sleepForCommitInterval();
      }
    }
  }
  public Map.Entry<Long,HashMap<Long, log.Instance>> runPreparePhase(long ballot) {
    var state = new PrepareState(this.id);
    multipaxos.PrepareRequest.Builder request = multipaxos.PrepareRequest.newBuilder();
    request.setSender(this.id);
    request.setBallot(ballot);
    for (var peer : rpcPeers) {
      threadPool.submit(() -> {
        multipaxos.PrepareResponse response;
        try {
          response = multipaxos.MultiPaxosRPCGrpc.newBlockingStub(peer.stub).prepare(request.build());
          logger.info(id + " sent prepare request to " + peer.id);
        } catch (StatusRuntimeException e) {
          logger.info(id + " RPC connection failed to " + peer.id);
          state.mu.lock();
          state.numRpcs++;
          state.cv.signal();
          state.mu.unlock();
          return;
        }
        state.mu.lock();
        ++state.numRpcs;
        if (response.getType() == multipaxos.ResponseType.OK) {
          ++state.numOks;
          for (int i = 0; i < response.getInstancesCount(); ++i) {
            state.maxLastIndex =max(state.maxLastIndex,response.getInstances(i).getIndex());
            insert(state.log, makeInstance(response.getInstances(i)));
          }
        } else {
          assert (response.getType() == multipaxos.ResponseType.REJECT);
          mu.lock();
          if (response.getBallot() > ballot) {
            becomeFollower(response.getBallot());
            state.leader = extractLeaderId(this.ballot);
          }
          mu.unlock();
        }
        state.cv.signal();
        state.mu.unlock();
      });
    }
    state.mu.lock();
    while (state.leader == this.id && state.numOks <= rpcPeers.size() / 2
            && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (state.numOks > rpcPeers.size() / 2) {
      state.mu.unlock();
      return new HashMap.SimpleEntry<>(state.maxLastIndex,state.log);
    }
    state.mu.unlock();
    return null;
  }
  public Result runAcceptPhase(long ballot, long index, command.Command command, long clientId) {
    var state = new AcceptState(this.id);
    log.Instance instance = new log.Instance();

    instance.setBallot(ballot);
    instance.setIndex(index);
    instance.setClientId(clientId);
    instance.setState(Instance.InstanceState.kInProgress);
    instance.setCommand(command);

    multipaxos.AcceptRequest.Builder request = multipaxos.AcceptRequest.newBuilder();
    request.setSender(this.id);
    request.setInstance(makeProtoInstance(instance));

    for (var peer : rpcPeers) {
      threadPool.submit(() -> {
        multipaxos.AcceptResponse response;
        try {
          response = multipaxos.MultiPaxosRPCGrpc.newBlockingStub(peer.stub).accept(request.build());
        } catch (StatusRuntimeException e) {
          logger.info(id + " RPC connection failed to " + peer.id);
          state.mu.lock();
          ++state.numRpcs;
          state.cv.signal();
          state.mu.unlock();
          return;
        }
        logger.info(id + " sent accept request to " + peer.id);
        state.mu.lock();
        ++state.numRpcs;
        if (response.getType() == multipaxos.ResponseType.OK) {
          ++state.numOks;
        } else {
          mu.lock();
          if (response.getBallot() > this.ballot) {
            becomeFollower(response.getBallot());
            state.leader = extractLeaderId(this.ballot);
          }
          mu.unlock();
        }
        state.cv.signal();
        state.mu.unlock();
      });
    }
    state.mu.lock();
    while (state.leader == this.id && state.numOks <= rpcPeers.size() / 2
            && state.numRpcs != rpcPeers.size()) {
      try {
        state.cv.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (state.numOks > rpcPeers.size() / 2) {
      log.commit(index);
      state.mu.unlock();
      return new Result(MultiPaxosResultType.kOk, null);
    }
    state.mu.unlock();
    if (state.leader != this.id) {
      return new Result(MultiPaxosResultType.kSomeoneElseLeader, state.leader);
    }
    return new Result(MultiPaxosResultType.kRetry, null);
  }
  public Long runCommitPhase(long ballot, long globalLastExecuted) {
    var state = new CommitState(this.id, log.getLastExecuted());
    multipaxos.CommitRequest.Builder request = multipaxos.CommitRequest.newBuilder();

    request.setBallot(ballot);
    request.setSender(this.id);
    request.setLastExecuted(state.minLastExecuted);
    request.setGlobalLastExecuted(globalLastExecuted);
    for (var peer : rpcPeers) {
      threadPool.submit(() -> {
        multipaxos.CommitResponse response;
        try {
          response = multipaxos.MultiPaxosRPCGrpc.newBlockingStub(peer.stub).commit(request.build());
          logger.info(id + " sent commit to " + peer.id);
        } catch (StatusRuntimeException e) {
          logger.info(id + " RPC connection failed to " + peer.id);
          state.mu.lock();
          state.numRpcs++;
          state.cv.signal();
          state.mu.unlock();
          return;
        }
        state.mu.lock();
        ++state.numRpcs;
        if (response.isInitialized()) {
          if (response.getType() == multipaxos.ResponseType.OK) {
            ++state.numOks;

            if (response.getLastExecuted() < state.minLastExecuted) {
              state.minLastExecuted = response.getLastExecuted();
            }
          } else {
            mu.lock();
            if (response.getBallot() > this.ballot) {
              becomeFollower(response.getBallot());
              state.leader = extractLeaderId(this.ballot);
            }
            mu.unlock();
          }
        }
        state.cv.signal();
        state.mu.unlock();
      });
    }
    state.mu.lock();
    while (state.leader == this.id && state.numRpcs != rpcPeers.size()) {
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
  public void replay(long ballot, HashMap<Long, log.Instance> log) {

    for (Map.Entry<Long, log.Instance> entry : log.entrySet()) {
      var r = runAcceptPhase(ballot, entry.getValue().getIndex(), entry.getValue().getCommand(),
              entry.getValue().getClientId());
      while(r.type == MultiPaxosResultType.kRetry)
        r = runAcceptPhase(ballot, entry.getValue().getIndex(), entry.getValue().getCommand(),entry.getValue().getClientId());
      if (r.type == MultiPaxosResultType.kSomeoneElseLeader) {
        return;
      }
    }
  }
  @Override
  public void prepare(multipaxos.PrepareRequest request, StreamObserver<multipaxos.PrepareResponse> responseObserver) {
    mu.lock();
    logger.info(id + " <-- prepare-- " + request.getSender());
    multipaxos.PrepareResponse.Builder responseBuilder = multipaxos.PrepareResponse.newBuilder();
    try {
      if (request.getBallot() > ballot) {
        becomeFollower(request.getBallot());

        for (var i : log.instances()) {
//          if (i == null) {
//            logger.info("null instance of log");
//            continue;
//          }
          responseBuilder.addInstances(makeProtoInstance(i));
        }
        responseBuilder = responseBuilder.setType(multipaxos.ResponseType.OK);
      } else {
        responseBuilder = responseBuilder.setBallot(ballot).setType(multipaxos.ResponseType.REJECT);
      }
      var response = responseBuilder.build();
      logger.info("sending response : " + response);
      responseObserver.onNext(response);
      responseObserver.onCompleted();

    } finally {
      mu.unlock();
    }
  }
  @Override
  public void accept(multipaxos.AcceptRequest request, StreamObserver<multipaxos.AcceptResponse> responseObserver) {
    mu.lock();
    logger.info(this.id + " <--accept---  " + request.getSender());
    multipaxos.AcceptResponse.Builder responseBuilder = multipaxos.AcceptResponse.newBuilder();
    try {
      if (request.getInstance().getBallot() >= this.ballot) {
        becomeFollower(request.getInstance().getBallot());
        log.append(makeInstance(request.getInstance()));
        responseBuilder = responseBuilder.setType(multipaxos.ResponseType.OK);
      } else {
        responseBuilder = responseBuilder.setBallot(ballot).setType(multipaxos.ResponseType.REJECT);
      }
      var response = responseBuilder.build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

  public void commit(multipaxos.CommitRequest commitRequest,
                     StreamObserver<multipaxos.CommitResponse> responseObserver) {
    mu.lock();
    logger.info(id + " <--commit--- " + commitRequest.getSender());
    try {
      var response = multipaxos.CommitResponse.newBuilder();
      if (commitRequest.getBallot() >= ballot) {
        commitReceived.set(true);
        becomeFollower(commitRequest.getBallot());
        log.commitUntil(commitRequest.getLastExecuted(), commitRequest.getBallot());
        log.trimUntil(commitRequest.getGlobalLastExecuted());
        response.setLastExecuted(log.getLastExecuted());
        response.setType(multipaxos.ResponseType.OK);
      } else {
        response.setBallot(this.ballot);
        response.setType(multipaxos.ResponseType.REJECT);
      }
      response.setLastExecuted(log.getLastExecuted());
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

  public static long extractLeaderId(long ballot) {
    return ballot & kIdBits;
  }
  public static boolean isLeader(long ballot, long id) {
    return extractLeaderId(ballot) == id;
  }public static boolean isSomeoneElseLeader(long ballot, long id) {
    return !isLeader(ballot, id) && extractLeaderId(ballot) < kMaxNumPeers;
  }

  public long getId() {
    return id;
  }

  public long ballot() {
    mu.lock();
    try {
      return this.ballot;
    } finally {
      mu.unlock();
    }
  }
  public long nextBallot() {
    mu.lock();
    try {
      long nextBallot = ballot;
      nextBallot += kRoundIncrement;
      nextBallot = (nextBallot & ~kIdBits) | id;
      return nextBallot;
    } finally {
      mu.unlock();
    }
  }
  public void becomeLeader(long newBallot, long newLastIndex) {
    mu.lock();
    try {
      logger.info(id + " became a leader: ballot: " + ballot + " -> " + newBallot);
      ballot = newBallot;
      log.setLastIndex(newLastIndex);
      cvLeader.signal();
    } finally {
      mu.unlock();
    }
  }
  public void becomeFollower(long newBallot) {
    var oldLeaderId = extractLeaderId(ballot);
    var newLeaderId = extractLeaderId(newBallot);
    if (newLeaderId != id && (oldLeaderId == id || oldLeaderId == kMaxNumPeers)) {
      logger.info(id + " became a follower: ballot: " + ballot + " -> " + newBallot);
      mu.lock();
      cvFollower.signal();
      mu.unlock();
    }
    ballot = newBallot;
  }
  public void sleepForCommitInterval() {
    try {
      Thread.sleep(commitInterval);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  public void sleepForRandomInterval() {
    Random random = new Random();
    var sleepTime = random.nextInt(0, (int) commitInterval/2);
    try {
      Thread.sleep(commitInterval + commitInterval / 2 + sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  public boolean receivedCommit() {
    return commitReceived.compareAndExchange(true, false);
  }

  public static multipaxos.Command makeProtoCommand(command.Command command) {
    multipaxos.CommandType commandType = null;
    switch (command.getCommandType()) {
      case Del -> commandType = DEL;
      case Get -> commandType = GET;
      case Put -> commandType = PUT;
    }
    return multipaxos.Command.newBuilder().setType(commandType).setKey(command.getKey())
        .setValue(command.getValue()).build();
  }

  public static multipaxos.Instance makeProtoInstance(log.Instance inst) {
    multipaxos.InstanceState state = null;
    switch (inst.getState()) {
      case kInProgress -> state = multipaxos.InstanceState.INPROGRESS;
      case kCommitted -> state = multipaxos.InstanceState.COMMITTED;
      case kExecuted -> state = multipaxos.InstanceState.EXECUTED;
    }

    return multipaxos.Instance.newBuilder().setBallot(inst.getBallot()).setIndex(inst.getIndex())
        .setClientId(inst.getClientId()).setState(state)
        .setCommand(makeProtoCommand(inst.getCommand())).build();
  }

  public static command.Command makeCommand(multipaxos.Command cmd) {
    command.Command command = new command.Command();
    command.Command.CommandType commandType = null;
    switch (cmd.getType()) {
      case DEL -> commandType = Del;
      case GET -> commandType = Get;
      case PUT -> commandType = Put;
      case UNRECOGNIZED -> {
      }
    }
    command.setCommandType(commandType);
    command.setKey(cmd.getKey());
    command.setValue(cmd.getValue());
    return command;
  }

  public static log.Instance makeInstance(multipaxos.Instance inst) {
    log.Instance instance = new log.Instance();
    instance.setBallot(inst.getBallot());
    instance.setIndex(inst.getIndex());
    instance.setClientId(inst.getClientId());
    instance.setCommand(makeCommand(inst.getCommand()));
    return instance;
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

}

