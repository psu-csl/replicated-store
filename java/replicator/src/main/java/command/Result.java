package command;

public class Result {

  private boolean ok;
  private String value;

  public Result(boolean ok, String value) {
    this.ok = ok;
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public boolean isOk() {
    return ok;
  }

  public void setOk(boolean ok) {
    this.ok = ok;
  }
}