package paxos;


import static multipaxos.ResponseType.OK;
import static multipaxos.ResponseType.REJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static paxos.MultiPaxos.kMaxNumPeers;
import static paxos.MultiPaxos.makeProtoInstance;
import static paxos.MultiPaxosResultType.kOk;
import static paxos.MultiPaxosResultType.kRetry;
import static paxos.MultiPaxosResultType.kSomeoneElseLeader;
import static util.TestUtil.makeInstance;

import command.Command;
import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import kvstore.MemKVStore;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxos.AcceptRequest;
import multipaxos.AcceptResponse;
import multipaxos.CommitRequest;
import multipaxos.CommitResponse;
import multipaxos.MultiPaxosRPCGrpc;
import multipaxos.MultiPaxosRPCGrpc.MultiPaxosRPCBlockingStub;
import multipaxos.PrepareRequest;
import multipaxos.PrepareResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiPaxosTest {

  private static final int kNumPeers = 3;

  protected List<Configuration> configs;
  protected List<Log> logs;
  protected List<MultiPaxos> peers;
  protected List<MemKVStore> stores;


  public static MultiPaxosRPCGrpc.MultiPaxosRPCBlockingStub makeStub(String target) {
    /*ManagedChannel channel = ManagedChannelBuilder.forAddress(target, port)
        .usePlaintext().build();*/
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    return MultiPaxosRPCGrpc.newBlockingStub(channel);
  }

  public static CommitResponse sendCommit(MultiPaxosRPCBlockingStub stub, long ballot,
      long lastExecuted, long globalLastExecuted) {

    CommitRequest request = CommitRequest.newBuilder().setBallot(ballot)
        .setLastExecuted(lastExecuted).setGlobalLastExecuted(globalLastExecuted).build();
    return stub.commit(request);
  }

  public static PrepareResponse sendPrepare(MultiPaxosRPCBlockingStub stub, long ballot) {
    PrepareRequest request = PrepareRequest.newBuilder().setBallot(ballot).build();
    return stub.prepare(request);
  }

  public static AcceptResponse sendAccept(MultiPaxosRPCBlockingStub stub, Instance inst) {
    AcceptRequest request = AcceptRequest.newBuilder().setInstance(makeProtoInstance(inst)).build();
    return stub.accept(request);
  }

  public static long leader(MultiPaxos peer) {
    var r = peer.ballot();
    return MultiPaxos.leader(r.ballot);
  }

  public static boolean isLeader(MultiPaxos peer) {
    var r = peer.ballot();
    return MultiPaxos.isLeader(r.ballot, peer.getId());
  }

  public static boolean isSomeoneElseLeader(MultiPaxos peer) {
    return !isLeader(peer) && leader(peer) < kMaxNumPeers;
  }

  public Configuration makeConfig(long id, int port) {
    assert (id < kNumPeers);
    Configuration config = new Configuration();
    config.setId(id);
    config.setPort(port);
    config.setCommitPause(300);
    config.setThreadPoolSize(8);
    List<String> peers = new ArrayList<>();
    for (int i = 0; i < kNumPeers; i++) {
      peers.add("127.0.0.1:300" + i);
    }
    config.setPeers(peers);
    return config;
  }


  @BeforeEach
  void setUp() {
    this.configs = new ArrayList<>();
    this.logs = new ArrayList<>();
    this.peers = new ArrayList<>();
    this.stores = new ArrayList<>();

    for (int i = 0; i < kNumPeers; i++) {
      var config = makeConfig(i, 3000 + i);
      var log = new Log();
      var peer = new MultiPaxos(log, config);

      configs.add(config);
      logs.add(log);
      peers.add(peer);
      stores.add(new MemKVStore());
    }
  }


  @Test
  @Order(0)
  void constructor() {
    assertEquals(kMaxNumPeers, leader(peers.get(0)));
    assertFalse(isLeader(peers.get(0)));
    assertFalse(isSomeoneElseLeader(peers.get(0)));
  }

  @Test
  @Order(1)
  void nextBallot() {

    int ballot = 2;

    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peers.get(2).nextBallot());
    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peers.get(2).nextBallot());

    assertTrue(isLeader(peers.get(2)));
    assertFalse(isSomeoneElseLeader(peers.get(2)));
    assertEquals(2, leader(peers.get(2)));
  }


  @Test
  @Order(2)
  void requestsWithLowerBallotIgnored() {
    peers.get(0).startRPCServer();
    // var stub = makeStub("localhost", configs.get(0).getPort());
    var stub = makeStub(configs.get(0).getPeers().get(0));

    peers.get(0).nextBallot();
    peers.get(0).nextBallot();

    var staleBallot = peers.get(1).nextBallot();

    var r1 = sendPrepare(stub, staleBallot);
    assertEquals(REJECT, r1.getType());
    assertTrue(isLeader(peers.get(0)));

    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(staleBallot, index);
    var r2 = sendAccept(stub, instance);
    assertEquals(REJECT, r2.getType());
    assertTrue(isLeader(peers.get(0)));
    assertNull(logs.get(0).get(index));

    var r3 = sendCommit(stub, staleBallot, 0, 0);
    assertEquals(REJECT, r3.getType());
    assertTrue(isLeader(peers.get(0)));

    peers.get(0).stopRPCServer();
  }

  @Test
  @Order(3)
  void requestsWithHigherBallotChangeLeaderToFollower() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    peers.get(0).nextBallot();
    assertTrue(isLeader(peers.get(0)));
    var r1 = sendPrepare(stub, peers.get(1).nextBallot());
    assertEquals(OK, r1.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(0).nextBallot();
    assertTrue(isLeader(peers.get(0)));
    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(peers.get(1).nextBallot(), index);
    var r2 = sendAccept(stub, instance);
    assertEquals(OK, r2.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(0).nextBallot();
    assertTrue(isLeader(peers.get(0)));
    var r3 = sendCommit(stub, peers.get(1).nextBallot(), 0, 0);
    assertEquals(OK, r3.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(0).stopRPCServer();
  }

  @Test
  @Order(4)
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

    var r1 = sendCommit(stub, ballot, index2, 0);
    assertEquals(OK, r1.getType());
    assertEquals(0, r1.getLastExecuted());
    assertTrue(logs.get(0).get(index1).isCommitted());
    assertTrue(logs.get(0).get(index2).isCommitted());
    assertTrue(logs.get(0).get(index3).isInProgress());

    logs.get(0).execute(stores.get(0));
    logs.get(0).execute(stores.get(0));

    var r2 = sendCommit(stub, ballot, index2, index2);
    assertEquals(index2, r2.getLastExecuted());
    assertEquals(OK, r2.getType());
    assertNull(logs.get(0).get(index1));
    assertNull(logs.get(0).get(index2));
    assertTrue(logs.get(0).get(index3).isInProgress());

    peers.get(0).stopRPCServer();
  }

  @Test
  @Order(5)
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

    var r1 = sendPrepare(stub, ballot);
    assertEquals(OK, r1.getType());
    assertEquals(3, r1.getInstancesCount());
    assertEquals(instance1, MultiPaxos.makeInstance(r1.getInstances(0)));
    assertEquals(instance2, MultiPaxos.makeInstance(r1.getInstances(1)));
    assertEquals(instance3, MultiPaxos.makeInstance(r1.getInstances(2)));

    var r2 = sendCommit(stub, ballot, index2, 0);
    assertEquals(OK, r2.getType());

    logs.get(0).execute(stores.get(0));
    logs.get(0).execute(stores.get(0));

    var r3 = sendPrepare(stub, ballot);
    assertEquals(OK, r3.getType());
    assertEquals(3, r3.getInstancesCount());
    assertSame(r3.getInstances(0).getState(), multipaxos.InstanceState.EXECUTED);
    assertSame(r3.getInstances(1).getState(), multipaxos.InstanceState.EXECUTED);
    assertEquals(instance3, MultiPaxos.makeInstance(r3.getInstances(2)));

    var r4 = sendCommit(stub, ballot, index2, 2);
    assertEquals(OK, r4.getType());

    var r5 = sendPrepare(stub, ballot);
    assertEquals(OK, r5.getType());
    assertEquals(1, r5.getInstancesCount());
    assertEquals(instance3, MultiPaxos.makeInstance(r5.getInstances(0)));

    peers.get(0).stopRPCServer();
  }

  @Test
  @Order(6)
  void acceptAppendsToLog() {
    peers.get(0).startRPCServer();
    var stub = makeStub(configs.get(0).getPeers().get(0));

    var ballot = peers.get(0).nextBallot();
    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);

    var r1 = sendAccept(stub, instance1);
    assertEquals(OK, r1.getType());
    assertEquals(instance1, logs.get(0).get(index1));
    assertNull(logs.get(0).get(index2));

    var r2 = sendAccept(stub, instance2);
    assertEquals(OK, r2.getType());
    assertEquals(instance1, logs.get(0).get(index1));
    assertEquals(instance2, logs.get(0).get(index2));

    peers.get(0).stopRPCServer();
  }

  @Test
  @Order(7)
  void prepareResponseWithHigherBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(1).nextBallot();
    var peer2Ballot = peers.get(2).nextBallot();

    var r = sendCommit(stub1, peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peers.get(0).runPreparePhase(peer0Ballot);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  @Order(8)
  public void acceptResponseWithHighBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(1).nextBallot();
    var peer2Ballot = peers.get(2).nextBallot();

    var cr = sendCommit(stub1, peer2Ballot, 0, 0);
    assertEquals(OK, cr.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    var ar = peers.get(0).runAcceptPhase(peer0Ballot, 1, new Command(), 0);
    assertEquals(kSomeoneElseLeader, ar.type);
    assertEquals(2, ar.leader);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  @Order(9)
  public void commitResponseWithHigherBallotChangesLeaderToFollower() {
    peers.get(0).startRPCServer();
    peers.get(1).startRPCServer();
    peers.get(2).startRPCServer();
    var stub1 = makeStub(configs.get(0).getPeers().get(1));

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(1).nextBallot();
    var peer2Ballot = peers.get(2).nextBallot();

    var r = sendCommit(stub1, peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peers.get(0).runCommitPhase(peer0Ballot, 0);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
    peers.get(2).stopRPCServer();
  }

  @Test
  @Order(10)
  public void runPreparePhase() throws InterruptedException {
    peers.get(0).startRPCServer();

    var peer0Ballot = peers.get(0).nextBallot();
    var peer1Ballot = peers.get(1).nextBallot();

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
    peer1Ballot = peers.get(1).nextBallot();

    var peer0i5 = makeInstance(peer0Ballot, index5, InstanceState.kInProgress, CommandType.Get);
    var peer1i5 = makeInstance(peer1Ballot, index5, InstanceState.kInProgress, CommandType.Put);

    logs.get(0).append(peer0i5);
    logs.get(1).append(peer1i5);

    var ballot = peers.get(0).nextBallot();

    assertNull(peers.get(0).runPreparePhase(ballot));

    peers.get(1).startRPCServer();

    Thread.sleep(2000);

    var log = peers.get(0).runPreparePhase(ballot);

    assertEquals(i1, log.get(index1));
    assertEquals(i2, log.get(index2));
    assertEquals(peer0i3.getCommand(), log.get(index3).getCommand());
    assertEquals(peer0i4.getCommand(), log.get(index4).getCommand());
    assertEquals(peer1i5, log.get(index5));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
  }

  @Test
  @Order(11)
  void runAcceptPhase() throws InterruptedException {
    peers.get(0).startRPCServer();

    var ballot = peers.get(0).nextBallot();
    var index = logs.get(0).advanceLastIndex();

    var result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kRetry, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).get(index).isInProgress());
    assertNull(logs.get(1).get(index));
    assertNull(logs.get(2).get(index));

    peers.get(1).startRPCServer();

    Thread.sleep(2000);

    result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kOk, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).get(index).isCommitted());
    assertTrue(logs.get(1).get(index).isInProgress());
    assertNull(logs.get(2).get(index));

    peers.get(0).stopRPCServer();
    peers.get(1).stopRPCServer();
  }

}

