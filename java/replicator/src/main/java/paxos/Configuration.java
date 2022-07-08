package paxos;

import java.util.List;

public class Configuration {

  private long id;
  private int port;
  private long heartbeatPause;
  private int threadPoolSize;
  private List<String> peers;
  private int heartbeatDelta;

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

  public long getHeartbeatPause() {
    return heartbeatPause;
  }

  public void setHeartbeatPause(long heartbeatPause) {
    this.heartbeatPause = heartbeatPause;
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

  public int getHeartbeatDelta() {
    return heartbeatDelta;
  }

  public void setHeartbeatDelta(int heartbeatDelta) {
    this.heartbeatDelta = heartbeatDelta;
  }
}
