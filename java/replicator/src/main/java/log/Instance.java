package log;

import command.Command;

public class Instance {

  private Command command;

  private long round;
  private long index;
  private long clientId;
  private InstanceState state;

  public Command getCommand() {
    return command;
  }

  public void setCommand(Command command) {
    this.command = command;
  }

  public long getRound() {
    return round;
  }

  public void setRound(long round) {
    this.round = round;
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