package paxos;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import log.Log;

public class MultiPaxos {

  protected static final long kIdBits = 0xff;
  protected static final long kRoundIncrement = kIdBits + 1;
  protected static long kMaxNumPeers = 0xf;
  private final AtomicLong ballot;
  private final ReentrantLock mu;
  private long id;

  public MultiPaxos(Log log, Configuration config) {
    this.id = config.getId();
    this.ballot = new AtomicLong(kMaxNumPeers);
    mu = new ReentrantLock();
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
}
