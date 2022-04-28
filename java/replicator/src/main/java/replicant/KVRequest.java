package replicant;

import command.Command;
import command.Command.CommandType;

public class KVRequest {

  public String value;
  public String key;
  public CommandType commandType;

  @Override
  public String toString() {
    return "{\"Key\" : " + key + "\n\"Value\" : " + value + "\n\"CommandType :\"" + commandType
        + "\n}";
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public CommandType getCommandType() {
    return this.commandType;
  }
}
