package command;

public class Command {

  private CommandType type;
  private String key;
  private String value;

  public Command() {
  }

  public Command(CommandType type, String key, String value) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public CommandType getCommandType() {
    return type;
  }

  public void setCommandType(CommandType type) {
    this.type = type;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Command c)) {
      return false;
    }
    return c.getCommandType() == this.type && c.getKey().equals(this.key) && c.getValue()
        .equals(this.value);
  }

  public enum CommandType {
    kGet, kPut, kDel
  }


}