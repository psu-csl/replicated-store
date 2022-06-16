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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiPaxosTest {

  protected Log log;
  protected MultiPaxos multiPaxos;
  protected Configuration config;

  protected ExecutorService server = Executors.newSingleThreadExecutor();


  public Instance MakeInstance(long ballot, long index, InstanceState state, CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }

  @BeforeEach
  void setUp() {
    config = new Configuration();
    config.setId(0);
    config.setPort(8080);
    log = new Log();
    multiPaxos = new MultiPaxos(log, config);
    server.submit(() -> {
      multiPaxos.startServer();
      multiPaxos.blockUntilShutDown();
    });

  }

  @AfterEach
  void tearDown() {
    multiPaxos.stopServer();
  }

  @Test
  void constructor() {
    assertEquals(MultiPaxos.kMaxNumPeers, multiPaxos.leader());
    assertFalse(multiPaxos.isLeader());
    assertFalse(multiPaxos.isSomeoneElseLeader());
  }

  @Test
  void nextBallot() {
    for (long id = 0; id < MultiPaxos.kMaxNumPeers; id++) {
      Configuration config = new Configuration();
      config.setId(id);
      MultiPaxos mp = new MultiPaxos(log, config);
      long ballot = id;

      ballot += MultiPaxos.kRoundIncrement;
      assertEquals(ballot, mp.nextBallot());
      ballot += MultiPaxos.kRoundIncrement;
      assertEquals(ballot, mp.nextBallot());

      assertTrue(mp.isLeader());
      assertFalse(mp.isSomeoneElseLeader());
      assertEquals(id, mp.leader());
    }
  }

  @Test
  void heartbeatHandlerSameBallot() {

    log.append(MakeInstance(17, 1, InstanceState.kExecuted, CommandType.kPut));
    log.append(MakeInstance(17, 2, InstanceState.kInProgress, CommandType.kGet));
    log.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(17).setLastExecuted(2)
        .setGlobalLastExecuted(1)
        .build();
    HeartbeatResponse response = blockingStub.heartbeat(request);
    // System.out.println("Response is " + response.getLastExecuted());

    assertEquals(1, response.getLastExecuted());
    assertEquals(log.get(2L).getState(), InstanceState.kCommitted);
    assertNull(log.get(1L));

  }

  @Test
  void heartbeatHandlerHigherBallot() {
    var inst1 = MakeInstance(17, 1, InstanceState.kExecuted, CommandType.kPut);
    var inst2 = MakeInstance(17, 2, InstanceState.kInProgress, CommandType.kGet);
    var inst3 = MakeInstance(17, 3, InstanceState.kInProgress, CommandType.kDel);
    log.append(inst1);
    log.append(inst2);
    log.append(inst3);
    log.setLastExecuted(1);

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext()
        .build();
    var blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(18).setLastExecuted(3)
        .setGlobalLastExecuted(1)
        .build();
    HeartbeatResponse response = blockingStub.heartbeat(request);

    assertEquals(1, response.getLastExecuted());
    assertEquals(log.get(2L).getState(), InstanceState.kInProgress);
    assertEquals(log.get(3L).getState(), InstanceState.kInProgress);
    assertNull(log.get(1L));

  }
}