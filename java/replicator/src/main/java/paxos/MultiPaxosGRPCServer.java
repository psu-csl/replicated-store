package paxos;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;

public class MultiPaxosGRPCServer {

  private final int port;
  private final Server server;

  public MultiPaxosGRPCServer(int port, MultiPaxos multiPaxos) {
    this.port = port;
    server = ServerBuilder.forPort(port).addService(new MultiPaxosGRPC(multiPaxos)).build();
  }

  public void start() throws IOException {
    server.start();
    System.out.println("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        MultiPaxosGRPCServer.this.stop();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));
  }

  private void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private void blockUntilShutDown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }


  private static class MultiPaxosGRPC extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

    private final MultiPaxos multiPaxos;

    public MultiPaxosGRPC(MultiPaxos multiPaxos) {
      this.multiPaxos = multiPaxos;
    }

    @Override
    public void heartbeat(HeartbeatRequest heartbeatRequest,
        StreamObserver<HeartbeatResponse> responseObserver) {
      responseObserver.onNext(multiPaxos.heartbeatHandler(heartbeatRequest));
      responseObserver.onCompleted();
    }

  }
}

