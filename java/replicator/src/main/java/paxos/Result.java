package paxos;

public class Result {

  public MultiPaxosResultType type;
  public Long leader;

  public Result(MultiPaxosResultType type, Long leader) {
    this.type = type;
    this.leader = leader;
  }
}
