package multipaxos;


import static multipaxos.ResponseType.OK;
import static multipaxos.ResponseType.REJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static multipaxos.MultiPaxos.kMaxNumPeers;
import static multipaxos.MultiPaxos.kRoundIncrement;
import static multipaxos.MultiPaxos.makeProtoInstance;
import static multipaxos.MultiPaxosResultType.kOk;
import static multipaxos.MultiPaxosResultType.kRetry;
import static multipaxos.MultiPaxosResultType.kSomeoneElseLeader;
import static util.TestUtil.makeConfig;
import static util.TestUtil.makeInstance;

import command.Command;
import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import kvstore.MemKVStore;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxos.MultiPaxosRPCGrpc.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class MultiPaxosTest {

  private static final int kNumPeers = 3;
  protected List<Configuration> configs;
  protected List<Log> logs;
  protected List<MultiPaxos> peers;
  protected List<MemKVStore> stores;

  public static Stub makeStub(String target) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    return new Stub(channel, multipaxos.MultiPaxosRPCGrpc.newBlockingStub(channel));
  }

  public static multipaxos.CommitResponse sendCommit(MultiPaxosRPCBlockingStub stub, long ballot,
                                                     long lastExecuted, long globalLastExecuted) {

    multipaxos.CommitRequest request = multipaxos.CommitRequest.newBuilder().setBallot(ballot)
        .setLastExecuted(lastExecuted).setGlobalLastExecuted(globalLastExecuted).build();
    return stub.commit(request);
  }

  public static multipaxos.PrepareResponse sendPrepare(MultiPaxosRPCBlockingStub stub, long ballot) {
    multipaxos.PrepareRequest request = multipaxos.PrepareRequest.newBuilder().setBallot(ballot).build();
    return stub.prepare(request);
  }

  public static multipaxos.AcceptResponse sendAccept(MultiPaxosRPCBlockingStub stub, Instance inst) {
    multipaxos.AcceptRequest request = multipaxos.AcceptRequest.newBuilder().setInstance(makeProtoInstance(inst)).build();
    return stub.accept(request);
  }

  public static long leader(MultiPaxos peer) {
    return MultiPaxos.leader(peer.ballot());
  }

  public static boolean isLeader(MultiPaxos peer) {
    return MultiPaxos.isLeader(peer.ballot(), peer.getId());
  }

  public static boolean isSomeoneElseLeader(MultiPaxos peer) {
    return !isLeader(peer) && leader(peer) < kMaxNumPeers;
  }

  public Long oneLeader() {
    var leader = leader(peers.get(0));
    var numLeaders = 0;

    for (var p : peers) {
      if (isLeader(p)) {
        ++numLeaders;
        if (numLeaders > 1 || p.getId() != leader) {
          return null;
        }
      } else if (leader(p) != leader) {
        return null;
      }
    }
    return leader;
  }

  @BeforeEach
  void setUp() {
    this.configs = new ArrayList<>();
    this.logs = new ArrayList<>();
    this.peers = new ArrayList<>();
    this.stores = new ArrayList<>();

    for (int i = 0; i < kNumPeers; i++) {
      var config = makeConfig(i, kNumPeers);
      stores.add(new MemKVStore());
      var log = new Log(stores.get(i));
      var peer = new MultiPaxos(log, config);
      configs.add(config);
      logs.add(log);
      peers.add(peer);

    }
  }

  @Test
  void constructor() {
    assertEquals(kMaxNumPeers, leader(peers.get(0)));
    assertFalse(isLeader(peers.get(0)));
    assertFalse(isSomeoneElseLeader(peers.get(0)));
  }

  @Test
  void nextBallot() {
    for (int id = 0; id < kNumPeers; ++id) {
      var ballot = id;
      ballot += kRoundIncrement;
      assertEquals(ballot, peers.get(id).nextBallot());
    }
  }

  @Test
  void requestsWithLowerBallotIgnored() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    peers.get(0).becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    peers.get(0).becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());

    var staleBallot = peers.get(1).nextBallot();

    var r1 = sendPrepare(stub.get(), staleBallot);
    assertEquals(REJECT, r1.getType());
    assertTrue(isLeader(peers.get(0)));

    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(staleBallot, index);
    var r2 = sendAccept(stub.get(), instance);
    assertEquals(REJECT, r2.getType());
    assertTrue(isLeader(peers.get(0)));
    assertNull(logs.get(0).at(index));

    var r3 = sendCommit(stub.get(), staleBallot, 0, 0);
    assertEquals(REJECT, r3.getType());
    assertTrue(isLeader(peers.get(0)));

    stub.close();
    peers.get(0).stopRPCServer();
  }

  @Test
  void requestsWithHigherBallotChangeLeaderToFollower() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    peers.get(0).becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var r1 = sendPrepare(stub.get(), peers.get(1).nextBallot());
    assertEquals(OK, r1.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(1).becomeLeader(peers.get(1).nextBallot(), logs.get(0).getLastIndex());
    peers.get(0).becomeLeader(peers.get(0).nextBallot(),logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(peers.get(1).nextBallot(), index);
    var r2 = sendAccept(stub.get(), instance);
    assertEquals(OK, r2.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(1).becomeLeader(peers.get(1).nextBallot(),logs.get(1).getLastIndex());
    peers.get(0).becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var r3 = sendCommit(stub.get(), peers.get(1).nextBallot(), 0, 0);
    assertEquals(OK, r3.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    stub.close();
    peers.get(0).stopRPCServer();
  }

  @Test
  void commitCommitsAndTrims() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    var ballot = peers.get(0).nextBallot();
    var index1 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index1));
    var index2 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index2));
    var index3 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index3));

    var r1 = sendCommit(stub.get(), ballot, index2, 0);
    assertEquals(OK, r1.getType());
    assertEquals(0, r1.getLastExecuted());
    assertTrue(logs.get(0).at(index1).isCommitted());
    assertTrue(logs.get(0).at(index2).isCommitted());
    assertTrue(logs.get(0).at(index3).isInProgress());

    logs.get(0).execute();
    logs.get(0).execute();

    var r2 = sendCommit(stub.get(), ballot, index2, index2);
    assertEquals(index2, r2.getLastExecuted());
    assertEquals(OK, r2.getType());
    assertNull(logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));
    assertTrue(logs.get(0).at(index3).isInProgress());

    stub.close();
    peers.get(0).stopRPCServer();
  }

  @Test
  void prepareRespondsWithCorrectInstances() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    var ballot = peers.get(0).nextBallot();

    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    logs.get(0).append(instance1);

    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);
    logs.get(0).append(instance2);

    var index3 = logs.get(0).advanceLastIndex();
    var instance3 = makeInstance(ballot, index3);
    logs.get(0).append(instance3);

    var r1 = sendPrepare(stub.get(), ballot);
    assertEquals(OK, r1.getType());
    assertEquals(3, r1.getInstancesCount());
    assertEquals(instance1, MultiPaxos.makeInstance(r1.getInstances(0)));
    assertEquals(instance2, MultiPaxos.makeInstance(r1.getInstances(1)));
    assertEquals(instance3, MultiPaxos.makeInstance(r1.getInstances(2)));

    var r2 = sendCommit(stub.get(), ballot, index2, 0);
    assertEquals(OK, r2.getType());

    logs.get(0).execute();
    logs.get(0).execute();

    ballot = peers.get(0).nextBallot();
    var r3 = sendPrepare(stub.get(), ballot);
    assertEquals(OK, r3.getType());
    assertEquals(3, r3.getInstancesCount());
    assertSame(r3.getInstances(0).getState(), multipaxos.InstanceState.EXECUTED);
    assertSame(r3.getInstances(1).getState(), multipaxos.InstanceState.EXECUTED);
    assertEquals(instance3, MultiPaxos.makeInstance(r3.getInstances(2)));

    var r4 = sendCommit(stub.get(), ballot, index2, 2);
    assertEquals(OK, r4.getType());

    ballot = peers.get(0).nextBallot();

    var r5 = sendPrepare(stub.get(), ballot);
    assertEquals(OK, r5.getType());
    assertEquals(1, r5.getInstancesCount());
    assertEquals(instance3, MultiPaxos.makeInstance(r5.getInstances(0)));

    stub.close();
    peers.get(0).stopRPCServer();
  }

  @Test
  void acceptAppendsToLog() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    var ballot = peers.get(0).nextBallot();
    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);

    var r1 = sendAccept(stub.get(), instance1);
    assertEquals(OK, r1.getType());
    assertEquals(instance1, logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));

    var r2 = sendAccept(stub.get(), instance2);
    assertEquals(OK, r2.getType());
    assertEquals(instance1, logs.get(0).at(index1));
    assertEquals(instance2, logs.get(0).at(index2));
    stub.close();
    peers.get(0).stopRPCServer();
  }

  @Test
  void prepareResponseWithHigherBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    peers.get(1).becomeLeader(peers.get(1).nextBallot(), logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot, logs.get(2).getLastIndex());

    var r = sendCommit(stub1.get(), peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peers.get(0).runPreparePhase(peer0Ballot);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    stub1.close();
    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  public void acceptResponseWithHighBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot,logs.get(0).getLastIndex());
    peers.get(1).becomeLeader(peers.get(1).nextBallot(),logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot,logs.get(2).getLastIndex());

    var cr = sendCommit(stub1.get(), peer2Ballot, 0, 0);
    assertEquals(OK, cr.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    var ar = peers.get(0).runAcceptPhase(peer0Ballot, 1, new Command(), 0);
    assertEquals(kSomeoneElseLeader, ar.type);
    assertEquals(2, ar.leader);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    stub1.close();
    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  public void commitResponseWithHigherBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot,logs.get(0).getLastIndex());
    peers.get(1).becomeLeader(peers.get(1).nextBallot(),logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot,logs.get(2).getLastIndex());

    var r = sendCommit(stub1.get(), peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peers.get(0).runCommitPhase(peer0Ballot, 0);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    stub1.close();
    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  public void runPreparePhase() throws InterruptedException {
    peers.get(0).startRPCServer();

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot,logs.get(0).getLastIndex());
    var peer1Ballot = peers.get(1).nextBallot();
    peers.get(1).becomeLeader(peer1Ballot,logs.get(1).getLastIndex());

    long index1 = 1;
    var i1 = makeInstance(peer0Ballot, index1, CommandType.Put);

    logs.get(0).append(i1);
    logs.get(1).append(i1);

    long index2 = 2;
    var i2 = makeInstance(peer0Ballot, index2);

    logs.get(1).append(i2);

    long index3 = 3;
    var peer0i3 = makeInstance(peer0Ballot, index3, InstanceState.kCommitted, CommandType.Del);
    var peer1i3 = makeInstance(peer1Ballot, index3, InstanceState.kInProgress, CommandType.Del);

    logs.get(0).append(peer0i3);
    logs.get(1).append(peer1i3);

    long index4 = 4;
    var peer0i4 = makeInstance(peer0Ballot, index4, InstanceState.kExecuted, CommandType.Del);
    var peer1i4 = makeInstance(peer1Ballot, index4, InstanceState.kInProgress, CommandType.Del);

    logs.get(0).append(peer0i4);
    logs.get(1).append(peer1i4);

    long index5 = 5;
    peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot,logs.get(0).getLastIndex());
    peer1Ballot = peers.get(1).nextBallot();
    peers.get(1).becomeLeader(peer1Ballot,logs.get(1).getLastIndex());

    var peer0i5 = makeInstance(peer0Ballot, index5, InstanceState.kInProgress, CommandType.Get);
    var peer1i5 = makeInstance(peer1Ballot, index5, InstanceState.kInProgress, CommandType.Put);

    logs.get(0).append(peer0i5);
    logs.get(1).append(peer1i5);

    var ballot = peers.get(0).nextBallot();

    assertNull(peers.get(0).runPreparePhase(ballot));

    peers.get(1).startRPCServer();

    Thread.sleep(2000);

    ballot = peers.get(0).nextBallot();

    var res = peers.get(0).runPreparePhase(ballot);
    var log = res.getValue();
    var lastIndex = res.getKey();
    assertEquals(lastIndex,5);
    assertEquals(i1, log.get(index1));
    assertEquals(i2, log.get(index2));
    assertEquals(peer0i3.getCommand(), log.get(index3).getCommand());
    assertEquals(peer0i4.getCommand(), log.get(index4).getCommand());
    assertEquals(peer1i5, log.get(index5));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
  }

  @Test
  void runAcceptPhase() throws InterruptedException {
    peers.get(0).startRPCServer();

    var ballot = peers.get(0).nextBallot();
    var index = logs.get(0).advanceLastIndex();

    var result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kRetry, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).at(index).isInProgress());
    assertNull(logs.get(1).at(index));
    assertNull(logs.get(2).at(index));

    peers.get(1).startRPCServer();

    Thread.sleep(2000);

    result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kOk, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).at(index).isCommitted());
    assertTrue(logs.get(1).at(index).isInProgress());
    assertNull(logs.get(2).at(index));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
  }

  @Test
  void runCommitPhase() throws InterruptedException {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();

    var ballot = peers.get(0).nextBallot();

    for (long index = 1; index <= 3; ++index) {
      for (int peer = 0; peer < kNumPeers; ++peer) {
        if (index == 3 && peer == 2) {
          continue;
        }
        logs.get(peer).append(makeInstance(ballot, index, InstanceState.kCommitted));
        logs.get(peer).execute();
      }
    }
    long gle = 0;
    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(0, gle);

    peers.get(2).startRPCServer();

    logs.get(2).append(makeInstance(ballot, 3));

    Thread.sleep(2000);

    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(2, gle);

    logs.get(2).execute();

    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(3, gle);

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  void replay() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();

    var ballot = peers.get(0).nextBallot();

    long index1 = 1;
    var i1 = makeInstance(ballot, index1, InstanceState.kCommitted, CommandType.Put);
    long index2 = 2;
    var i2 = makeInstance(ballot, index2, InstanceState.kExecuted, CommandType.Get);
    long index3 = 3;
    var i3 = makeInstance(ballot, index3, InstanceState.kInProgress, CommandType.Del);

    HashMap<Long, log.Instance> log = new HashMap<>();
    log.put(index1, i1);
    log.put(index2, i2);
    log.put(index3, i3);

    assertNull(logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));
    assertNull(logs.get(0).at(index3));

    assertNull(logs.get(1).at(index1));
    assertNull(logs.get(1).at(index2));
    assertNull(logs.get(1).at(index3));

    var newBallot = peers.get(0).nextBallot();
    peers.get(0).replay(newBallot, log);

    i1.setBallot(newBallot);
    i2.setBallot(newBallot);
    i3.setBallot(newBallot);

    i1.setState(InstanceState.kCommitted);
    i2.setState(InstanceState.kCommitted);
    i3.setState(InstanceState.kCommitted);

    assertEquals(i1, logs.get(0).at(index1));
    assertEquals(i2, logs.get(0).at(index2));
    assertEquals(i3, logs.get(0).at(index3));

    i1.setState(InstanceState.kInProgress);
    i2.setState(InstanceState.kInProgress);
    i3.setState(InstanceState.kInProgress);

    assertEquals(i1, logs.get(1).at(index1));
    assertEquals(i2, logs.get(1).at(index2));
    assertEquals(i3, logs.get(1).at(index3));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
  }

  @Test
  void replicate() throws InterruptedException {
    peers.get(0).start();

    var result = peers.get(0).replicate(new Command(), 0);
    assertEquals(kRetry, result.type);
    assertNull(result.leader);

    peers.get(1).start();
    peers.get(2).start();

    long commitInterval = configs.get(0).getCommitInterval();
    var commitInterval3x = 3 * commitInterval;

    Thread.sleep(commitInterval3x);

    var leader = oneLeader();
    assertNotNull(leader);

    result = peers.get(Math.toIntExact(leader)).replicate(new Command(), 0);
    assertEquals(kOk, result.type);
    assertNull(result.leader);

    var nonLeader = (leader + 1) % kNumPeers;

    result = peers.get((int) nonLeader).replicate(new Command(), 0);
    assertEquals(kSomeoneElseLeader, result.type);
    assertEquals(leader, result.leader);

    peers.get(0).stop();
    peers.get(1).stop();
    peers.get(2).stop();
  }

  static class Stub {

    private final ManagedChannel channel;
    private final MultiPaxosRPCBlockingStub stub;

    public Stub(ManagedChannel channel, MultiPaxosRPCBlockingStub stub) {
      this.channel = channel;
      this.stub = stub;
    }

    public MultiPaxosRPCBlockingStub get() {
      return stub;
    }

    public void close() {
      channel.shutdown();
    }
  }
}

