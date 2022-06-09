package paxos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import command.Command;
import command.Command.CommandType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kvstore.MemKVStore;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;
import multipaxosrpc.MultiPaxosRPCGrpc.MultiPaxosRPCBlockingStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiPaxosGRPCServerTest {

  private MultiPaxosGRPCServer multiPaxosGRPCServer;
  private Log log;

  public Instance MakeInstance(long ballot, long index, InstanceState state, CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }


  @BeforeEach
  void setUp() {
    log = new Log();
    MemKVStore store = new MemKVStore();
    log.append(MakeInstance(17, 1, InstanceState.kInProgress, CommandType.kPut));
    log.append(MakeInstance(17, 2, InstanceState.kInProgress, CommandType.kGet));
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(() -> {
      log.execute(store);
      log.execute(store);
    });
  }

  @Test
  void simpleTest() {
    Configuration configuration = new Configuration();
    configuration.setId(0);
    configuration.setPort(8080);
    MultiPaxos multiPaxos = new MultiPaxos(log, configuration);
    multiPaxosGRPCServer = new MultiPaxosGRPCServer(configuration.getPort(), multiPaxos);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    var f = executor.submit(() -> {
      try {
        multiPaxosGRPCServer.start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", configuration.getPort())
        .usePlaintext().build();
    MultiPaxosRPCBlockingStub blockingStub = MultiPaxosRPCGrpc.newBlockingStub(channel);

    HeartbeatRequest request = HeartbeatRequest.newBuilder().setBallot(17).setLastExecuted(7)
        .setGlobalLastExecuted(1)
        .build();
    HeartbeatResponse response = blockingStub.heartbeat(request);
    // System.out.println("Response is " + response.getLastExecuted());
    assertEquals(2, response.getLastExecuted());
    try {
      f.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

  }

}