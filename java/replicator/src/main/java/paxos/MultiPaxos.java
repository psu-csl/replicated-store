package paxos;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;
import multipaxosrpc.HeartbeatRequest;
import multipaxosrpc.HeartbeatResponse;

public class MultiPaxos {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static long kMaxNumPeers = 0xf;
  private final AtomicLong ballot;
  private final ReentrantLock mu;
  private final Log log;
  private final MultiPaxosGRPCServer multiPaxosGRPC;
  private long id;
  private Instant lastHeartbeat;

  public MultiPaxos(Log log, Configuration config) {
    this.id = config.getId();
    this.ballot = new AtomicLong(kMaxNumPeers);
    this.log = log;
    mu = new ReentrantLock();
    multiPaxosGRPC = new MultiPaxosGRPCServer(config.getPort(), this);
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

  public HeartbeatResponse heartbeatHandler(HeartbeatRequest msg) {
    mu.lock();
    try {
      if (msg.getBallot() >= ballot.get()) {
        lastHeartbeat = Instant.now();
        ballot.set(msg.getBallot());
        log.commitUntil(msg.getLastExecuted(), ballot.get());
        log.trimUntil(msg.getGlobalLastExecuted());
      }
      return HeartbeatResponse.newBuilder().setLastExecuted(log.getLastExecuted()).build();
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


}
