package multipaxos.network;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class CommitRequest {

  private long ballot;
  private long lastExecuted;
  private long globalLastExecuted;
  private long sender;

  @JsonCreator
  public CommitRequest(@JsonProperty("ballot") long ballot,
                       @JsonProperty("lastExecuted") long lastExecuted,
                       @JsonProperty("globalLastExecuted") long globalLastExecuted,
                       @JsonProperty("sender") long sender) {
    this.ballot = ballot;
    this.lastExecuted = lastExecuted;
    this.globalLastExecuted = globalLastExecuted;
    this.sender = sender;
  }

  public long getBallot() {
    return ballot;
  }

  public void setBallot(long ballot) {
    this.ballot = ballot;
  }

  public long getLastExecuted() {
    return lastExecuted;
  }

  public void setLastExecuted(long lastExecuted) {
    this.lastExecuted = lastExecuted;
  }

  public long getGlobalLastExecuted() {
    return globalLastExecuted;
  }

  public void setGlobalLastExecuted(long globalLastExecuted) {
    this.globalLastExecuted = globalLastExecuted;
  }

  public long getSender() {
    return sender;
  }

  public void setSender(long sender) {
    this.sender = sender;
  }
}
