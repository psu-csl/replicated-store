package paxos;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;

public class MultiPaxos extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static final long kMaxNumPeers = 0xf;
  private final ReentrantLock mu;

  private final Condition cvLeader;
  private Log log;
  private Server server;
  private Instant lastHeartbeat;
  private long heartbeatPause;
  private long id;
  private long ballot;

  public MultiPaxos(Log log, Configuration config) {
    this.id = config.getId();
    this.ballot = kMaxNumPeers;
    this.heartbeatPause = config.getHeartbeatPause();
    this.log = log;
    mu = new ReentrantLock();
    cvLeader = mu.newCondition();
    server = ServerBuilder.forPort(config.getPort()).addService(this).build();
  }

  public long nextBallot() {
    mu.lock();
    try {
      ballot += kRoundIncrement;
      ballot = (ballot & ~kIdBits) | id;
      cvLeader.signalAll();
      return ballot;
    } finally {
      mu.unlock();
    }
  }

  public long leader() {
    mu.lock();
    try {
      return ballot & kIdBits;
    } finally {
      mu.unlock();
    }
  }

  public boolean isLeader() {
    mu.lock();
    try {
      return isLeaderLockless();
    } finally {
      mu.unlock();
    }
  }

  public boolean isLeaderLockless() {
    return (ballot & kIdBits) == id;
  }

  public boolean isSomeoneElseLeader() {
    mu.lock();
    try {
      var id = ballot & kIdBits;
      return id != this.id && id < kMaxNumPeers;
    } finally {
      mu.unlock();
    }
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void startServer() {
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Server started listening on port: " + server.getPort());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stopServer();
      System.out.println("Server is successfully shut down");
    }));
  }

  public void blockUntilShutDown() {
    if (server != null) {
      try {
        server.awaitTermination();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void stopServer() {
    if (server != null) {
      try {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest,
      StreamObserver<HeartbeatResponse> responseObserver) {

    mu.lock();
    try {
      if (heartbeatRequest.getBallot() >= ballot) {
        lastHeartbeat = Instant.now();
        ballot = heartbeatRequest.getBallot();

        log.commitUntil(heartbeatRequest.getLastExecuted(), heartbeatRequest.getBallot());
        log.trimUntil(heartbeatRequest.getGlobalLastExecuted());

      }
      HeartbeatResponse response = HeartbeatResponse.newBuilder()
          .setLastExecuted(log.getLastExecuted()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } finally {
      mu.unlock();
    }
  }

}
