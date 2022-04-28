package command;


/*
Command :
    - CommandID
    - Key
    - Value
    - Type
 */

public class Command {

  // TODO : ? not sure what to set commandID
  //private long commandID;
  private final String key;
  private final String value;
  private final String commandType;

  public Command(String key, String value, String type) {
    this.key = key;
    this.value = value;
    this.commandType = type;
  }

  public String getKey() {
    return this.key;
  }

  public String getValue() {
    return this.value;
  }

  public String getCommandType() {
    return this.commandType;
  }

}

