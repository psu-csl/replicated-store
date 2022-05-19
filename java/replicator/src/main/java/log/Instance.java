package log;

import command.Command;

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

  public boolean isInProgress() {
    return state == InstanceState.kInProgress;
  }

  public boolean isCommited() {
    return state == InstanceState.kCommitted;
  }

  public boolean isExecuted() {
    return state == InstanceState.kExecuted;
  }

  public void setCommited() {
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

  public enum InstanceState {
    kInProgress, kCommitted, kExecuted
  }

}
