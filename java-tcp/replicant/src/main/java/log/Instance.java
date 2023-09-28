package log;

import command.Command;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Instance {

  private Command command;
  private long ballot;
  private long index;
  private long clientId;
  private InstanceState state;

  public Instance() {
    ballot = 0;
    index = 0;
    clientId = 0;
    state = InstanceState.kInProgress;
  }

  public Instance(long ballot, long index, long clientId, InstanceState state, Command command) {
    this.ballot = ballot;
    this.index = index;
    this.clientId = clientId;
    this.state = state;
    this.command = command;
  }

  public boolean isInProgress() {
    return state == InstanceState.kInProgress;
  }

  public boolean isCommitted() {
    return state == InstanceState.kCommitted;
  }

  public boolean isExecuted() {
    return state == InstanceState.kExecuted;
  }

  public void setCommitted() {
    state = InstanceState.kCommitted;
  }

  public void setExecuted() {
    state = InstanceState.kExecuted;
  }

  public Command getCommand() {
    return command;
  }

  public void setCommand(Command command) {
    this.command = command;
  }

  public long getBallot() {
    return ballot;
  }

  public void setBallot(long ballot) {
    this.ballot = ballot;
  }

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public long getClientId() {
    return clientId;
  }

  public void setClientId(long clientId) {
    this.clientId = clientId;
  }

  public InstanceState getState() {
    return state;
  }

  public void setState(InstanceState state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "ballot : " + ballot + " index : " + index + " clientId : " + clientId + " state : "
        + state + "\n";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    final Instance inst = (Instance) o;
    return this.ballot == inst.getBallot() && this.command.equals(inst.getCommand())
        && this.state == inst.getState() && this.index == inst.getIndex()
        && this.clientId == inst.getClientId();

  }

  public enum InstanceState {
    kInProgress, kCommitted, kExecuted
  }
}
