package paxos;

import java.util.List;

public class Configuration {

  private long id;
  private int port;
  private long commitPause;
  private int threadPoolSize;
  private List<String> peers;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public long getCommitPause() {
    return commitPause;
  }

  public void setCommitPause(long commitPause) {
    this.commitPause = commitPause;
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
