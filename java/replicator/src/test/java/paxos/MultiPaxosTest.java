package paxos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import command.Command;
import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
class MultiPaxosTest {

  protected Log log0, log1, log2;
  protected MultiPaxos peer0, peer1, peer2;
  protected Configuration config0, config1, config2;

  public Instance makeInstance(long ballot, long index, InstanceState state, CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }

  public Configuration makeConfig(long id, int port) {
    Configuration config = new Configuration();
    config.setId(id);
    config.setPort(port);
    config.setHeartbeatPause(300);
    return config;
  }


  @BeforeEach
  void setUp() {
   /* config = new Configuration();
    config.setId(0);
    config.setPort(8080);
    log = new Log();
    multiPaxos = new MultiPaxos(log, config);
    server.submit(() -> {
      multiPaxos.startServer();
      multiPaxos.blockUntilShutDown();
    });
*/
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
    assertEquals(MultiPaxos.kMaxNumPeers, peer0.leader());
    assertFalse(peer0.isLeader());
    assertFalse(peer0.isSomeoneElseLeader());
  }

  @Test
  void nextBallot() {

    int ballot = 2;

    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());
    ballot += MultiPaxos.kRoundIncrement;
    assertEquals(ballot, peer2.nextBallot());

    assertTrue(peer2.isLeader());
    assertFalse(peer2.isSomeoneElseLeader());
    assertEquals(2, peer2.leader());
  }

  @Test
  void heartbeatHandlerSameBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.startServer();
      peer0.blockUntilShutDown();
    });
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
    // System.out.println("Response is " + response.getLastExecuted());

    assertEquals(1, response.getLastExecuted());
    assertEquals(log0.get(2L).getState(), InstanceState.kCommitted);
    assertNull(log0.get(1L));
    peer0.stopServer();
  }

  @Test
  void heartbeatHandlerHigherBallot() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.startServer();
      peer0.blockUntilShutDown();
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

    peer0.stopServer();
  }

  @Test
  void heartbeatIgnoreStaleRPC() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.startServer();
      peer0.blockUntilShutDown();
    });
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    peer0.nextBallot();
    peer0.nextBallot();

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(peer1.nextBallot()).build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertTrue(peer0.isLeader());
    peer0.stopServer();
  }

  @Test
  void heartbeatChangesLeaderToFollower() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      peer0.startServer();
      peer0.blockUntilShutDown();
    });
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", config0.getPort())
        .usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    peer0.nextBallot();
    HeartbeatRequest request0 = HeartbeatRequest.newBuilder().setBallot(peer1.nextBallot()).build();
    HeartbeatResponse response = blockingStub.heartbeat(request0);

    assertFalse(peer0.isLeader());
    assertEquals(1, peer0.leader());
    peer0.stopServer();
  }

  @Test
  void heartbeatUpdatesLeaderOnFollowers() throws InterruptedException {
    ExecutorService executor0 = Executors.newSingleThreadExecutor();
    ExecutorService executor1 = Executors.newSingleThreadExecutor();
    ExecutorService executor2 = Executors.newSingleThreadExecutor();

    executor0.submit(() -> {
      peer0.startServer();
      peer0.blockUntilShutDown();
    });
    executor1.submit(() -> {
      peer1.startServer();
      peer1.blockUntilShutDown();
    });
    executor2.submit(() -> {
      peer2.startServer();
      peer2.blockUntilShutDown();
    });

    var pause = 2 * config0.getHeartbeatPause();

    assertFalse(peer0.isLeader());
    assertFalse(peer1.isLeader());
    assertFalse(peer2.isLeader());

    peer0.nextBallot();
    Thread.sleep(1000);
    assertTrue(peer0.isLeader());
    assertFalse(peer1.isLeader());
    assertEquals(0, peer1.leader());
    assertFalse(peer2.isLeader());
    assertEquals(0, peer2.leader());

    peer1.nextBallot();
    Thread.sleep(pause);
    assertTrue(peer1.isLeader());
    assertFalse(peer0.isLeader());
    assertEquals(1, peer0.leader());
    assertFalse(peer2.isLeader());
    assertEquals(1, peer2.leader());

    peer2.nextBallot();
    Thread.sleep(pause);
    assertTrue(peer2.isLeader());
    assertFalse(peer0.isLeader());
    assertEquals(2, peer0.leader());
    assertFalse(peer1.isLeader());
    assertEquals(2, peer2.leader());

    peer0.stopServer();
    peer1.stopServer();
    peer2.stopServer();
  }

}