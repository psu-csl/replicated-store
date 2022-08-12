package util;

import command.Command;
import command.Command.CommandType;
import log.Instance;
import log.Instance.InstanceState;

public class TestUtil {


  public static Instance makeInstance(long ballot, long index) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command());
  }

  public static Instance makeInstance(long ballot, long index, CommandType type) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command(type, "", ""));
  }

  public static Instance makeInstance(long ballot, long index, InstanceState state,
      CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }

}
