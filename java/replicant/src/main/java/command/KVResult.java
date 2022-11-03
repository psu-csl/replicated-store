package command;

public class KVResult {

  private final boolean ok;
  private final String value;

  public KVResult(boolean ok, String value) {
    this.ok = ok;
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public boolean isOk() {
    return ok;
  }

}