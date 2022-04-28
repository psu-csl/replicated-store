package command;

public class CommandResult {

  private final boolean isSuccess;
  private final String value;

  public CommandResult(boolean isSuccess, String value) {
    this.isSuccess = isSuccess;
    this.value = value;
  }

  public boolean isSuccessful() {
    return isSuccess;
  }

  public String getValue() {
    return value;
  }
}
