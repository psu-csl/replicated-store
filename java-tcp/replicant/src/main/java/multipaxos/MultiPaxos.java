package multipaxos;

import static multipaxos.network.ResponseType.OK;
import static multipaxos.network.ResponseType.REJECT;

import multipaxos.network.ChannelMap;
import multipaxos.network.Message.MessageType;
import multipaxos.network.PrepareRequest;
import multipaxos.network.PrepareResponse;
import multipaxos.network.AcceptRequest;
import multipaxos.network.AcceptResponse;
import multipaxos.network.CommitRequest;
import multipaxos.network.CommitResponse;

import ch.qos.logback.classic.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxos.network.TcpLink;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;
import log.Instance;

class Peer {

  public final long id;
  public final TcpLink stub;

  public Peer(long id, TcpLink stub) {
    this.id = id;
    this.stub = stub;
  }
}

public class MultiPaxos {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private static final Logger logger = (Logger) LoggerFactory.getLogger(MultiPaxos.class);
  private AtomicLong ballot;
  private final Log log;
  private final long id;
  private final AtomicBoolean commitReceived;
  private final long commitInterval;
  private final int numPeers;
  private final List<Peer> peers;
  private final AtomicLong nextChannelId;
  private final ChannelMap channels;
  private final ReentrantLock mu;
  private final ExecutorService threadPool;

  private final Condition cvLeader;
  private final Condition cvFollower;

  private final AtomicBoolean prepareThreadRunning;
  private final ExecutorService prepareThread;

  private final AtomicBoolean commitThreadRunning;
  private final ExecutorService commitThread;

  public MultiPaxos(Log log, Configuration config) {
    this.ballot = new AtomicLong(kMaxNumPeers);
    this.log = log;
    this.id = config.getId();
    commitReceived = new AtomicBoolean(false);
    this.commitInterval = config.getCommitInterval();
    this.numPeers = config.getPeers().size();
    this.nextChannelId = new AtomicLong(0);
    this.channels = new ChannelMap();

    threadPool = Executors.newFixedThreadPool(config.getThreadPoolSize());
    prepareThreadRunning = new AtomicBoolean(false);
    commitThreadRunning = new AtomicBoolean(false);

    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    cvFollower = mu.newCondition();
    prepareThread = Executors.newSingleThreadExecutor();
    commitThread = Executors.newSingleThreadExecutor();

    peers = new ArrayList<>();
    long peerId = 0;
    for (var address : config.getPeers()) {
      peers.add(new Peer(peerId++, new TcpLink(address, channels)));
    }
  }

  public long ballot() {
    return ballot.get();
  }

  public long nextBallot() {
    long nextBallot = ballot.get();
    nextBallot += kRoundIncrement;
    nextBallot = (nextBallot & ~kIdBits) | id;
    return nextBallot;
  }

  public void becomeLeader(long newBallot, long newLastIndex) {
    mu.lock();
    try {
      logger.debug(id + " became a leader: ballot: " + ballot + " -> " + newBallot);
      ballot.set(newBallot);
      log.setLastIndex(newLastIndex);
      cvLeader.signal();
    } finally {
      mu.unlock();
    }
  }

  public void becomeFollower(long newBallot) {
    mu.lock();
    try {
      if (newBallot <= ballot.get()) {
        return;
      }
      var oldLeaderId = extractLeaderId(ballot.get());
      var newLeaderId = extractLeaderId(newBallot);
      if (newLeaderId != id && (oldLeaderId == id || oldLeaderId == kMaxNumPeers)) {
        logger.debug(id + " became a follower: ballot: " + ballot + " -> " + newBallot);
        cvFollower.signal();
      }
      ballot.set(newBallot);
    } finally {
      mu.unlock();
    }
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
    var sleepTime = random.nextInt(0, (int) commitInterval / 2);
    try {
      Thread.sleep(commitInterval + commitInterval / 2 + sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean receivedCommit() {
    return commitReceived.compareAndExchange(true, false);
  }

  void prepareThread() {
    while (prepareThreadRunning.get()) {
      mu.lock();
      try {
        while (prepareThreadRunning.get() && isLeader(this.ballot.get(), this.id)) {
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
        try {
          var r = runPreparePhase(nextBallot);
          if (r != null) {
            var maxLastIndex = r.getKey();
            var log = r.getValue();
            becomeLeader(nextBallot, maxLastIndex);
            replay(nextBallot, log);
            break;
          }
        } catch (IOException | InterruptedException e) {
          logger.error(e.getMessage());
        }
      }
    }
  }

  public void commitThread() {
    while (commitThreadRunning.get()) {
      mu.lock();
      try {
        while (commitThreadRunning.get() && !isLeader(this.ballot.get(), this.id)) {
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
        try {
          gle = runCommitPhase(ballot, gle);
        } catch (IOException | InterruptedException e) {
          logger.error(e.getMessage());
        }
        sleepForCommitInterval();
      }
    }
  }

  public Map.Entry<Long, HashMap<Long, log.Instance>> runPreparePhase(long ballot)
      throws IOException, InterruptedException {
    var numOks = 0;
    var log = new HashMap<Long, log.Instance>();
    long maxLastIndex = 0;

    if (ballot > this.ballot.get()) {
      numOks++;
      log = this.log.getLog();
      maxLastIndex = this.log.getLastIndex();
    } else {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    var request = mapper.writeValueAsString(new PrepareRequest(ballot, id));
    var channelId = nextChannelId.addAndGet(1);
    var responseChan = addChannel(channelId);

    for (var peer : peers) {
      if (peer.id != this.id) {
        threadPool.submit(() -> {
          peer.stub.sendAwaitResponse(MessageType.PREPAREREQUEST, channelId,
              request);
          logger.debug(id + " sent prepare request to " + peer.id);
        });
      }
    }

    while (true) {
      var response = responseChan.take();
      var prepareResponse = mapper.readValue(response, PrepareResponse.class);
      if (prepareResponse.getType() == OK) {
        numOks++;
        for (var instance : prepareResponse.getLogs()) {
          if (instance.getIndex() > maxLastIndex) {
            maxLastIndex = instance.getIndex();
          }
          Log.insert(log, instance);
        }
      } else {
        becomeFollower(prepareResponse.getBallot());
        break;
      }
      if (numOks > numPeers / 2) {
        removeChannel(channelId);
        return new HashMap.SimpleEntry<>(maxLastIndex, log);
      }
    }
    removeChannel(channelId);
    return null;
  }

  public Result runAcceptPhase(long ballot, long index, command.Command command,
      long clientId) {
    var numOks = 0;

    var instance = new log.Instance();
    if (ballot == this.ballot.get()) {
      numOks++;
      instance.setBallot(ballot);
      instance.setIndex(index);
      instance.setClientId(clientId);
      instance.setState(Instance.InstanceState.kInProgress);
      instance.setCommand(command);
      log.append(instance);
      if (numOks > numPeers / 2) {
        log.commit(index);
        return new Result(MultiPaxosResultType.kOk, (long) -1);
      }
    } else {
      var currentLeaderId = extractLeaderId(this.ballot.get());
      return new Result(MultiPaxosResultType.kSomeoneElseLeader,
          currentLeaderId);
    }

    ObjectMapper mapper = new ObjectMapper();
    String request;
    try {
      request = mapper.writeValueAsString(new AcceptRequest(id, instance));
    } catch (IOException e) {
      logger.error(e.getMessage());
      return new Result(MultiPaxosResultType.kRetry, null);
    }
    var channelId = nextChannelId.addAndGet(1);
    var responseChan = addChannel(channelId);

    for (var peer : peers) {
      if (peer.id != this.id) {
        threadPool.submit(() -> {
          peer.stub.sendAwaitResponse(MessageType.ACCEPTREQUEST, channelId,
              request);
          logger.debug(id + " sent accept request to " + peer.id);
        });
      }
    }
    while (true) {
      try {
        var response = responseChan.take();
        var acceptResponse = mapper.readValue(response, AcceptResponse.class);
        if (acceptResponse.getType() == OK) {
          numOks++;
        } else {
          becomeFollower(acceptResponse.getBallot());
          break;
        }
      } catch (InterruptedException | IOException e) {
        logger.error(e.getMessage());
      }
      if (numOks > numPeers / 2) {
        log.commit(index);
        removeChannel(channelId);
        return new Result(MultiPaxosResultType.kOk, null);
      }
    }
    removeChannel(channelId);
    if (!isLeader(this.ballot.get(), id)) {
      return new Result(MultiPaxosResultType.kSomeoneElseLeader,
          extractLeaderId(ballot()));
    }
    return new Result(MultiPaxosResultType.kRetry, null);
  }

  public Long runCommitPhase(long ballot, long globalLastExecuted)
      throws IOException, InterruptedException {
    var numOks = 0;
    var minLastExecuted = log.getLastExecuted();

    numOks++;
    log.trimUntil(globalLastExecuted);
    if (numOks == numPeers) {
      return minLastExecuted;
    }

    ObjectMapper mapper = new ObjectMapper();
    var request = mapper.writeValueAsString(
        new CommitRequest(ballot, minLastExecuted, globalLastExecuted, id));
    var channelId = nextChannelId.addAndGet(1);
    var responseChan = addChannel(channelId);

    for (var peer : peers) {
      if (peer.id != this.id) {
        threadPool.submit(() -> {
          peer.stub.sendAwaitResponse(MessageType.COMMITREQUEST, channelId,
              request);
          logger.debug(id + " sent commit to " + peer.id);
        });
      }
    }

    while (true) {
      var response = responseChan.take();
      var commitResponse = mapper.readValue(response, CommitResponse.class);
      if (commitResponse.getType() == OK) {
        numOks++;
        if (commitResponse.getLastExecuted() < minLastExecuted) {
          minLastExecuted = commitResponse.getLastExecuted();
        }
      } else {
        becomeFollower(commitResponse.getBallot());
        break;
      }
      if (numOks == numPeers) {
        removeChannel(channelId);
        return minLastExecuted;
      }
    }
    removeChannel(channelId);
    return globalLastExecuted;
  }

  public void replay(long ballot, HashMap<Long, log.Instance> log) {
    for (Map.Entry<Long, log.Instance> entry : log.entrySet()) {
      var r = runAcceptPhase(ballot, entry.getValue().getIndex(),
          entry.getValue().getCommand(),
          entry.getValue().getClientId());
      while (r.type == MultiPaxosResultType.kRetry) {
        r = runAcceptPhase(ballot, entry.getValue().getIndex(),
            entry.getValue().getCommand(),
            entry.getValue().getClientId());
      }
      if (r.type == MultiPaxosResultType.kSomeoneElseLeader) {
        return;
      }
    }
  }

  public BlockingQueue<String> addChannel(long channelId) {
    BlockingQueue<String> channel = new LinkedBlockingDeque<>(numPeers - 1);
    channels.put(channelId, channel);
    return channel;
  }

  public void removeChannel(long channelId) {
    channels.delete(channelId);
  }

  public void start() {
    startPrepareThread();
    startCommitThread();
  }

  public void stop() {
    stopPrepareThread();
    stopCommitThread();

    threadPool.shutdown();
    try {
      threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void startPrepareThread() {
    logger.debug(id + " starting prepare thread");
    assert (!prepareThreadRunning.get());
    prepareThreadRunning.set(true);
    prepareThread.submit(this::prepareThread);
  }

  public void stopPrepareThread() {
    logger.debug(id + " stopping prepare thread");
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
    logger.debug(id + " starting commit thread");
    assert (!commitThreadRunning.get());
    commitThreadRunning.set(true);
    commitThread.submit(this::commitThread);
  }

  public void stopCommitThread() {
    logger.debug(id + " stopping commit thread");
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
      return new Result(MultiPaxosResultType.kSomeoneElseLeader,
          extractLeaderId(ballot));
    }
    return new Result(MultiPaxosResultType.kRetry, null);
  }

  public PrepareResponse prepare(PrepareRequest request) {
    logger.debug(id + " <-- prepare-- " + request.getSender());

    if (request.getBallot() > ballot.get()) {
      becomeFollower(request.getBallot());
      return new PrepareResponse(OK, ballot(), log.instances());
    } else {
      return new PrepareResponse(REJECT, ballot(), null);
    }
  }

  public AcceptResponse accept(AcceptRequest request) {
    logger.debug(this.id + " <--accept---  " + request.getSender());

    if (request.getInstance().getBallot() >= this.ballot()) {
      log.append(request.getInstance());
      if (request.getInstance().getBallot() > this.ballot.get()) {
        becomeFollower(request.getInstance().getBallot());
      }
      return new AcceptResponse(OK, ballot());
    }
    if (request.getInstance().getBallot() < this.ballot.get()) {
      return new AcceptResponse(REJECT, ballot());
    }
    return new AcceptResponse(OK, ballot());
  }

  public CommitResponse commit(CommitRequest request) {
    logger.debug(id + " <--commit--- " + request.getSender());

    if (request.getBallot() >= ballot()) {
      commitReceived.set(true);
      log.commitUntil(request.getLastExecuted(), request.getBallot());
      log.trimUntil(request.getGlobalLastExecuted());
      if (request.getBallot() > ballot.get()) {
        becomeFollower(request.getBallot());
      }
      return new CommitResponse(OK, ballot(), log.getLastExecuted());
    } else {
      return new CommitResponse(REJECT, ballot(), 0);
    }
  }

  public static long extractLeaderId(long ballot) {
    return ballot & kIdBits;
  }

  public static boolean isLeader(long ballot, long id) {
    return extractLeaderId(ballot) == id;
  }

  public static boolean isSomeoneElseLeader(long ballot, long id) {
    return !isLeader(ballot, id) && extractLeaderId(ballot) < kMaxNumPeers;
  }

  public long getId() {
    return id;
  }

  public List<Peer> getPeers() {
    return peers;
  }

  public long nextChannelId() {
    return this.nextChannelId.addAndGet(1);
  }
}

