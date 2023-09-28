package multipaxos.network;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class AcceptResponse {

  private ResponseType type;
  private long ballot;

  @JsonCreator
  public AcceptResponse(@JsonProperty("type") ResponseType type,
                        @JsonProperty("ballot") long ballot) {
    this.type = type;
    this.ballot = ballot;
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
}
