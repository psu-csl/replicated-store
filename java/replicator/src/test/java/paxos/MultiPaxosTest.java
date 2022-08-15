package paxos;


import static multipaxos.ResponseType.OK;
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
import java.util.Arrays;
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

  protected Log log0, log1, log2;
  protected MultiPaxos peer0, peer1, peer2;
  protected Configuration config0, config1, config2;
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
    Configuration config = new Configuration();
    config.setId(id);
    config.setPort(port);
    config.setHeartbeatPause(300);
    config.setThreadPoolSize(8);
    config.setHeartbeatDelta(10);
    config.setPeers(Arrays.asList("127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3002"));
    return config;
  }


  @BeforeEach
  void setUp() {

    config0 = makeConfig(0, 3000);
    config1 = makeConfig(1, 3001);
    config2 = makeConfig(2, 3002);

    log0 = new Log();
    log1 = new Log();
    log2 = new Log();

    peer0 = new MultiPaxos(log0, config0);
    peer1 = new MultiPaxos(log1, config1);
    peer2 = new MultiPaxos(log2, config2);

    store = new MemKVStore();
  }


  @Test
  @Order(0)
  void constructor() {
    assertEquals(kMaxNumPeers, leader(peer0));
    assertFalse(isLeader(peer0));
    assertFalse(isSomeoneElseLeader(peer0));
  }

  @Test
  @Order(1)
  void nextBallot() {

    int ballot = 2;

    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());
    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());

    assertTrue(isLeader(peer2));
    assertFalse(isSomeoneElseLeader(peer2));
    assertEquals(2, leader(peer2));
  }

  @Test
  @Order(2)
  void heartbeatHandlerHigherBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.start();
      // peer0.blockUntilShutDown();
    });

    var inst1 = makeInstance(17, 1, InstanceState.kExecuted, CommandType.Put);
    var inst2 = makeInstance(17, 2, InstanceState.kInProgress, CommandType.Get);
    var inst3 = makeInstance(17, 3, InstanceState.kInProgress, CommandType.Del);
    log0.append(inst1);
    log0.append(inst2);
    log0.append(inst3);
    log0.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext().build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(18).setLastExecuted(3)
        .setGlobalLastExecuted(1).build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(log0.get(2L).getState(), InstanceState.kInProgress);
    assertEquals(log0.get(3L).getState(), InstanceState.kInProgress);
    assertNull(log0.get(1L));

    peer0.stop();
    channel.shutdown();
    executor.shutdown();
  }

  @Test
  @Order(3)
  void heartbeatHandlerSameBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.start());
    log0.append(makeInstance(17, 1, InstanceState.kExecuted, CommandType.Put));
    log0.append(makeInstance(17, 2, InstanceState.kInProgress, CommandType.Get));
    log0.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext().build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(17).setLastExecuted(2)
        .setGlobalLastExecuted(1).build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(log0.get(2L).getState(), InstanceState.kCommitted);
    assertNull(log0.get(1L));
    peer0.stop();
    channel.shutdown();
    executor.shutdown();
  }


  @Test
  @Order(4)
  void heartbeatChangesLeaderToFollower() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.startRPCServer());

    var stub = makeStub("localhost", config0.getPort());
    peer0.nextBallot();

    sendHeartbeat(stub, peer1.nextBallot(), 0, 0);
    assertFalse(isLeader(peer0));
    assertEquals(1, leader(peer0));
    peer0.stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(5)
  void heartbeatIgnoreStaleRPC() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.startRPCServer());

    var stub = makeStub("localhost", config0.getPort());

    peer0.nextBallot();
    peer0.nextBallot();

    sendHeartbeat(stub, peer1.nextBallot(), 0, 0);
    assertTrue(isLeader(peer0));
    peer0.stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(6)
  void heartbeatCommitsAndTrims() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.startRPCServer());

    var stub = makeStub("localhost", config0.getPort());

    var ballot = peer0.nextBallot();

    var index1 = log0.advanceLastIndex();
    log0.append(makeInstance(ballot, index1));

    var index2 = log0.advanceLastIndex();
    log0.append(makeInstance(ballot, index2));

    var index3 = log0.advanceLastIndex();
    log0.append(makeInstance(ballot, index3));

    var r1 = sendHeartbeat(stub, ballot, index2, 0);
    assertEquals(0, r1.getLastExecuted());
    assertTrue(log0.get(index1).isCommitted());
    assertTrue(log0.get(index2).isCommitted());
    assertTrue(log0.get(index3).isInProgress());

    log0.execute(store);
    log0.execute(store);

    var r2 = sendHeartbeat(stub, ballot, index2, index2);
    assertEquals(index2, r2.getLastExecuted());

    assertNull(log0.get(index1));
    assertNull(log0.get(index2));
    assertTrue(log0.get(index3).isInProgress());

    peer0.stopRPCServer();
    executor.shutdown();
  }


  @Test
  @Order(7)
  void prepareRespondsWithCorrectInstances() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.startRPCServer());

    var stub = makeStub("localhost", config0.getPort());
    var ballot = peer0.nextBallot();
    var index1 = log0.advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    log0.append(instance1);

    var index2 = log0.advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);
    log0.append(instance2);

    var index3 = log0.advanceLastIndex();
    var instance3 = makeInstance(ballot, index3);
    log0.append(instance3);

    var r1 = sendPrepare(stub, ballot);
    assertEquals(OK, r1.getType());
    assertEquals(3, r1.getInstancesCount());
    //assertSame(instance1, response.getInstances(0));
    assertEquals(instance1, MultiPaxos.makeInstance(r1.getInstances(0)));
    assertEquals(instance2, MultiPaxos.makeInstance(r1.getInstances(1)));
    assertEquals(instance3, MultiPaxos.makeInstance(r1.getInstances(2)));

    sendHeartbeat(stub, ballot, index2, 0);
    log0.execute(store);
    log0.execute(store);

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

    peer0.stopRPCServer();
    executor.shutdown();
  }

  @Test
  @Order(8)
  void acceptAppendsToLog() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.startRPCServer());

    var ballot = peer0.nextBallot();

    var index1 = log0.advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);

    var index2 = log0.advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);

    var stub = makeStub("localhost", config0.getPort());

    var r1 = sendAccept(stub, instance1);

    assertEquals(OK, r1.getType());
    assertEquals(instance1, log0.get(index1));
    assertNull(log0.get(index2));

    var r2 = sendAccept(stub, instance2);

    assertEquals(OK, r2.getType());
    assertEquals(instance1, log0.get(index1));
    assertEquals(instance2, log0.get(index2));

    peer0.stopRPCServer();
    executor.shutdown();
  }
}