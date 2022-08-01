package paxos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static paxos.MultiPaxos.kMaxNumPeers;


import command.Command;
import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxos.HeartbeatRequest;
import multipaxos.HeartbeatResponse;
import multipaxos.MultiPaxosRPCGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
class MultiPaxosTest {

  protected Log log0, log1, log2;
  protected MultiPaxos peer0, peer1, peer2;
  protected Configuration config0, config1, config2;

  public static long leader(MultiPaxos peer){
    var r = peer.ballot();
    return MultiPaxos.leader(r.ballot);
  }

  public static boolean isLeader(MultiPaxos peer){
    var r = peer.ballot();
    return MultiPaxos.isLeader(r.ballot, peer.getId());
  }

  public static boolean isSomeoneElseLeader(MultiPaxos peer){
    return !isLeader(peer) && leader(peer) < kMaxNumPeers;
  }
  public Instance makeInstance(long ballot, long index, InstanceState state, CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
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
  }


  @Test
  void constructor() {
    assertEquals(kMaxNumPeers, leader(peer0));
    assertFalse(isLeader(peer0));
    assertFalse(isSomeoneElseLeader(peer0));
  }

  @Test
  void nextBallot() {

    int ballot = 2;

    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());
    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());

    assertTrue(isLeader(peer2));
    assertFalse(isSomeoneElseLeader(peer2));
    assertEquals(2,leader(peer2));
  }

  @Test
  void heartbeatHandlerSameBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.start());
    log0.append(makeInstance(17, 1, InstanceState.kExecuted, CommandType.kPut));
    log0.append(makeInstance(17, 2, InstanceState.kInProgress, CommandType.kGet));
    log0.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(17).setLastExecuted(2)
        .setGlobalLastExecuted(1)
        .build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(log0.get(2L).getState(), InstanceState.kCommitted);
    assertNull(log0.get(1L));
    peer0.stop();
    channel.shutdown();
  }

  @Test
  void heartbeatHandlerHigherBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.start();
      // peer0.blockUntilShutDown();
    });

    var inst1 = makeInstance(17, 1, InstanceState.kExecuted, CommandType.kPut);
    var inst2 = makeInstance(17, 2, InstanceState.kInProgress, CommandType.kGet);
    var inst3 = makeInstance(17, 3, InstanceState.kInProgress, CommandType.kDel);
    log0.append(inst1);
    log0.append(inst2);
    log0.append(inst3);
    log0.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(18).setLastExecuted(3)
        .setGlobalLastExecuted(1)
        .build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(log0.get(2L).getState(), InstanceState.kInProgress);
    assertEquals(log0.get(3L).getState(), InstanceState.kInProgress);
    assertNull(log0.get(1L));

    peer0.stop();
    channel.shutdown();
  }

  @Test
  void heartbeatIgnoreStaleRPC() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.start());
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    peer0.nextBallot();
    peer0.nextBallot();

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(peer1.nextBallot()).build();
    blockingStub.heartbeat(request);

    assertTrue(isLeader(peer0));
    peer0.stop();
    channel.shutdown();
  }

  @Test
  void heartbeatChangesLeaderToFollower() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> peer0.start());
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    peer0.nextBallot();
    HeartbeatRequest request0 = HeartbeatRequest.newBuilder().setBallot(peer1.nextBallot()).build();
    blockingStub.heartbeat(request0);

    assertFalse(isLeader(peer0));
    assertEquals(1, leader(peer0));
    peer0.stop();
    channel.shutdown();
  }

}