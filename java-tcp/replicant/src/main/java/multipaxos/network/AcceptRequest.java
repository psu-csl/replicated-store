package multipaxos.network;

import log.Instance;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class AcceptRequest {

  private Instance instance;
  private long sender;

  @JsonCreator
  public AcceptRequest(@JsonProperty("sender") long sender,
                       @JsonProperty("instance") Instance instance) {
    this.instance = instance;
    this.sender = sender;
  }

  public Instance getInstance() {
    return instance;
  }

  public void setInstance(Instance instance) {
    this.instance = instance;
  }

  public long getSender() {
    return sender;
  }

  public void setSender(long sender) {
    this.sender = sender;
  }
}
