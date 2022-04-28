package paxos;

import command.Command;
import command.Command.CommandType;
import command.Result;
import kvstore.KVStore;

public class DummyPaxos {

  private final KVStore kvStore;

  public DummyPaxos(KVStore kvStore) {
    this.kvStore = kvStore;
  }

  public Result agreeAndExecute(Command cmd) {
    CommandType cmdName = cmd.getCommandType();
    boolean isSuccess;
    Result dummyPaxosResult;
    switch (cmdName) {
      case kGet:
        String value = kvStore.get(cmd.getKey());
        dummyPaxosResult = new Result(true, value);
        break;
      case kPut:
        isSuccess = kvStore.put(cmd.getKey(), cmd.getValue());
        dummyPaxosResult = new Result(isSuccess, null);
        break;
      case kDel:
        isSuccess = kvStore.del(cmd.getKey());
        dummyPaxosResult = new Result(isSuccess, null);
        break;
      default:
        dummyPaxosResult = new Result(false, null);
    }

    return dummyPaxosResult;
  }
}
