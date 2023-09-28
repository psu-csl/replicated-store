package multipaxos.network;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class PrepareRequest {

  private long ballot;
  private long sender;

  @JsonCreator
  public PrepareRequest(@JsonProperty("ballot") long ballot,
                        @JsonProperty("sender") long sender) {
    this.ballot = ballot;
    this.sender = sender;
  }

  public long getBallot() {
    return ballot;
  }

  public void setBallot(long ballot) {
    this.ballot = ballot;
  }

  public long getSender() {
    return sender;
  }

  public void setSender(long sender) {
    this.sender = sender;
  }

}
