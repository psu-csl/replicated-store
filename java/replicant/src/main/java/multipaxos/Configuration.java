package multipaxos;

import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class Configuration {

  @JsonIgnore
  private long id;
  @JsonProperty("commit_interval")
  private long commitInterval;
  @JsonProperty("threadpool_size")
  private int threadPoolSize;
  @JsonProperty("peers")
  private List<String> peers;
  @JsonProperty("store")
  private boolean store;
  @JsonProperty("db_path")
  private  String rocksDBPath;
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

  public String getRocksDBPath() {
    return rocksDBPath;
  }

  public boolean isStore() {
    return store;
  }
}
