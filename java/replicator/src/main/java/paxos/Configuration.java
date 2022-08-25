package paxos;

import java.util.List;

public class Configuration {

  private long id;
  private long commitInterval;
  private int threadPoolSize;
  private List<String> peers;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getCommitInterval() {
    return commitInterval;
  }

  public void setCommitInterval(long commitInterval) {
    this.commitInterval = commitInterval;
  }

  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  public void setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }

  public List<String> getPeers() {
    return peers;
  }

  public void setPeers(List<String> peers) {
    this.peers = peers;
  }

}
