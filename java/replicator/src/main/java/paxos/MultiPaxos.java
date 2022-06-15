package paxos;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;
import multipaxosrpc.MultiPaxosRPCGrpc;

public class MultiPaxos extends MultiPaxosRPCGrpc.MultiPaxosRPCImplBase {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static long kMaxNumPeers = 0xf;
  private final AtomicLong ballot;
  private final ReentrantLock mu;
  private final Log log;
  private final Server server;
  private long id;
  private Instant lastHeartbeat;

  public MultiPaxos(Log log, Configuration config) {
    this.id = config.getId();
    this.ballot = new AtomicLong(kMaxNumPeers);
    this.log = log;
    mu = new ReentrantLock();
    server = ServerBuilder.forPort(config.getPort()).addService(this).build();
  }

  public long nextBallot() {
    mu.lock();
    try {
      ballot.addAndGet(kRoundIncrement);
      ballot.set((ballot.get() & (~kIdBits)) | this.id);
      return ballot.get();
    } finally {
      mu.unlock();
    }
  }

  public long leader() {
    mu.lock();
    try {
      return ballot.get() & kIdBits;
    } finally {
      mu.unlock();
    }
  }

  public boolean isLeader() {
    mu.lock();
    try {
      return leader() == id;
    } finally {
      mu.unlock();
    }
  }

  public boolean isSomeoneElseLeader() {
    var id = leader();
    return id != this.id && id < kMaxNumPeers;
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

    if (heartbeatRequest.getBallot() >= ballot.get()) {
      lastHeartbeat = Instant.now();
      ballot.set(heartbeatRequest.getBallot());
      log.commitUntil(heartbeatRequest.getLastExecuted(), ballot.get());
      log.trimUntil(heartbeatRequest.getGlobalLastExecuted());
    }
    HeartbeatResponse response = HeartbeatResponse.newBuilder()
        .setLastExecuted(log.getLastExecuted()).build();

    mu.unlock();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }


}
