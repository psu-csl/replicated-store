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
import static util.TestUtil.makeInstance;

import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kvstore.MemKVStore;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxos.AcceptRequest;
import multipaxos.AcceptResponse;
import multipaxos.HeartbeatRequest;
import multipaxos.HeartbeatResponse;
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
  protected MemKVStore store;


  public static MultiPaxosRPCGrpc.MultiPaxosRPCBlockingStub makeStub(String target, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(target, port)
        .usePlaintext().build();
    return MultiPaxosRPCGrpc.newBlockingStub(channel);
  }

  public static HeartbeatResponse sendHeartbeat(MultiPaxosRPCBlockingStub stub, long ballot,
      long lastExecuted, long globalLastExecuted) {

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(ballot)
        .setLastExecuted(lastExecuted).setGlobalLastExecuted(globalLastExecuted).build();
    return stub.heartbeat(request);
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
    config.setHeartbeatPause(300);
    config.setThreadPoolSize(8);
    config.setHeartbeatDelta(10);
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

    for (int i = 0; i < kNumPeers; i++) {
      var config = makeConfig(i, 3000 + i);
      var log = new Log();
      var peer = new MultiPaxos(log, config);

      configs.add(config);
      logs.add(log);
      peers.add(peer);
    }
    store = new MemKVStore();
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
  void heartbeatHandlerHigherBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peers.get(0).start();
      // peer0.blockUntilShutDown();
    });

    var inst1 = makeInstance(17, 1, InstanceState.kExecuted, CommandType.Put);
    var inst2 = makeInstance(17, 2, InstanceState.kInProgress, CommandType.Get);
    var inst3 = makeInstance(17, 3, InstanceState.kInProgress, CommandType.Del);
    logs.get(0).append(inst1);
    logs.get(0).append(inst2);
    logs.get(0).append(inst3);
    logs.get(0).setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", configs.get(0).getPort())
        .usePlaintext().build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(18).setLastExecuted(3)
        .setGlobalLastExecuted(1).build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(logs.get(0).get(2L).getState(), InstanceState.kInProgress);
    assertEquals(logs.get(0).get(3L).getState(), InstanceState.kInProgress);
    assertNull(logs.get(0).get(1L));

    peers.get(0).stop();
    channel.shutdown();
    executor.shutdown();
  }

  @Test
  @Order(3)
  void heartbeatHandlerSameBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).start());
    logs.get(0).append(makeInstance(17, 1, InstanceState.kExecuted, CommandType.Put));
    logs.get(0).append(makeInstance(17, 2, InstanceState.kInProgress, CommandType.Get));
    logs.get(0).setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", configs.get(0).getPort())
        .usePlaintext().build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(17).setLastExecuted(2)
        .setGlobalLastExecuted(1).build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(logs.get(0).get(2L).getState(), InstanceState.kCommitted);
    assertNull(logs.get(0).get(1L));
    peers.get(0).stop();
    channel.shutdown();
    executor.shutdown();
  }


  @Test
  @Order(4)
  void RPCSWithHigherBallotChangeLeaderToFollower() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).startRPCServer());

    var stub = makeStub("localhost", configs.get(0).getPort());
    peers.get(0).nextBallot();
    assertTrue(isLeader(peers.get(0)));

    sendHeartbeat(stub, peers.get(1).nextBallot(), 0, 0);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(0).nextBallot();
    assertTrue(isLeader(peers.get(0)));
    var r1 = sendPrepare(stub, peers.get(1).nextBallot());
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

    peers.get(0).stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(5)
  void RPCsWithLowerBallotIgnored() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).startRPCServer());

    var stub = makeStub("localhost", configs.get(0).getPort());

    peers.get(0).nextBallot();
    peers.get(0).nextBallot();

    var staleBallot = peers.get(1).nextBallot();

    sendHeartbeat(stub, staleBallot, 0, 0);
    assertTrue(isLeader(peers.get(0)));

    var r1 = sendPrepare(stub, staleBallot);
    assertEquals(REJECT, r1.getType());
    assertTrue(isLeader(peers.get(0)));

    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(staleBallot, index);
    var r2 = sendAccept(stub, instance);
    assertEquals(REJECT, r2.getType());
    assertTrue(isLeader(peers.get(0)));
    assertNull(logs.get(0).get(index));

    peers.get(0).stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(6)
  void heartbeatCommitsAndTrims() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).startRPCServer());

    var stub = makeStub("localhost", configs.get(0).getPort());

    var ballot = peers.get(0).nextBallot();

    var index1 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index1));

    var index2 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index2));

    var index3 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index3));

    var r1 = sendHeartbeat(stub, ballot, index2, 0);
    assertEquals(0, r1.getLastExecuted());
    assertTrue(logs.get(0).get(index1).isCommitted());
    assertTrue(logs.get(0).get(index2).isCommitted());
    assertTrue(logs.get(0).get(index3).isInProgress());

    logs.get(0).execute(store);
    logs.get(0).execute(store);

    var r2 = sendHeartbeat(stub, ballot, index2, index2);
    assertEquals(index2, r2.getLastExecuted());

    assertNull(logs.get(0).get(index1));
    assertNull(logs.get(0).get(index2));
    assertTrue(logs.get(0).get(index3).isInProgress());

    peers.get(0).stopRPCServer();
    executor.shutdown();
  }


  @Test
  @Order(7)
  void prepareRespondsWithCorrectInstances() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).startRPCServer());

    var stub = makeStub("localhost", configs.get(0).getPort());
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
    //assertSame(instance1, response.getInstances(0));
    assertEquals(instance1, MultiPaxos.makeInstance(r1.getInstances(0)));
    assertEquals(instance2, MultiPaxos.makeInstance(r1.getInstances(1)));
    assertEquals(instance3, MultiPaxos.makeInstance(r1.getInstances(2)));

    sendHeartbeat(stub, ballot, index2, 0);
    logs.get(0).execute(store);
    logs.get(0).execute(store);

    var r2 = sendPrepare(stub, ballot);
    assertEquals(OK, r2.getType());
    assertEquals(3, r2.getInstancesCount());
    assertSame(r2.getInstances(0).getState(), multipaxos.InstanceState.EXECUTED);
    assertSame(r2.getInstances(1).getState(), multipaxos.InstanceState.EXECUTED);
    assertEquals(instance3, MultiPaxos.makeInstance(r2.getInstances(2)));

    sendHeartbeat(stub, ballot, index2, 2);

    var r3 = sendPrepare(stub, ballot);
    assertEquals(OK, r3.getType());
    assertEquals(1, r3.getInstancesCount());
    assertEquals(instance3, MultiPaxos.makeInstance(r3.getInstances(0)));

    peers.get(0).stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(8)
  void acceptAppendsToLog() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peers.get(0).startRPCServer());

    var ballot = peers.get(0).nextBallot();

    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);

    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);

    var stub = makeStub("localhost", configs.get(0).getPort());

    var r1 = sendAccept(stub, instance1);

    assertEquals(OK, r1.getType());
    assertEquals(instance1, logs.get(0).get(index1));
    assertNull(logs.get(0).get(index2));

    var r2 = sendAccept(stub, instance2);

    assertEquals(OK, r2.getType());
    assertEquals(instance1, logs.get(0).get(index1));
    assertEquals(instance2, logs.get(0).get(index2));

    peers.get(0).stopRPCServer();
    executor.shutdown();
  }
}