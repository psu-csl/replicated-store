package paxos;

public class Configuration {

  private long id;
  private int port;

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
}
