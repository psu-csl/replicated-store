package paxos;

public class Configuration {

  private long id;
  private int port;
  private long heartbeatPause;

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
}
