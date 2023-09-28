package multipaxos.network;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class CommitResponse {

  private ResponseType type;
  private long ballot;
  private long lastExecuted;

  @JsonCreator
  public CommitResponse(@JsonProperty("type") ResponseType type,
                        @JsonProperty("ballot") long ballot,
                        @JsonProperty("lastExecuted") long lastExecuted) {
    this.type = type;
    this.ballot = ballot;
    this.lastExecuted = lastExecuted;
  }

  public ResponseType getType() {
    return type;
  }

  public void setType(ResponseType type) {
    this.type = type;
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
}
