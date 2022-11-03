package multipaxos;

public class Result {

  public final MultiPaxosResultType type;
  public final Long leader;

  public Result(MultiPaxosResultType type, Long leader) {
    this.type = type;
    this.leader = leader;
  }
}
