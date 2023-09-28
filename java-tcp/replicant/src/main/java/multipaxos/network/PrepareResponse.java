package multipaxos.network;

import java.util.List;
import log.Instance;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class PrepareResponse {

  private ResponseType type;
  private long ballot;
  private List<Instance> logs;

  @JsonCreator
  public PrepareResponse(@JsonProperty("type") ResponseType type,
                         @JsonProperty("ballot") long ballot,
                         @JsonProperty("logs") List<Instance> logs) {
    this.type = type;
    this.ballot = ballot;
    this.logs = logs;
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

  public List<Instance> getLogs() {
    return logs;
  }

  public void setLogs(List<Instance> logs) {
    this.logs = logs;
  }
}
