package paxos;

import command.Command;
import command.Command.CommandType;
import command.Result;
import kvstore.MemKVStore;

public class DummyPaxos {

  private final MemKVStore memStore;

  public DummyPaxos(MemKVStore memKVStore) {
    this.memStore = memKVStore;
  }

  public Result agreeAndExecute(Command cmd) {
    CommandType cmdName = cmd.getCommandType();
    boolean isSuccess;
    Result dummyPaxosResult;
    switch (cmdName) {
      case kGet -> {
        String value = memStore.get(cmd.getKey());
        dummyPaxosResult = new Result(true, value);
      }
      case kPut -> {
        isSuccess = memStore.put(cmd.getKey(), cmd.getValue());
        dummyPaxosResult = new Result(isSuccess, null);
      }
      case kDel -> {
        isSuccess = memStore.del(cmd.getKey());
        dummyPaxosResult = new Result(isSuccess, null);
      }
      default -> dummyPaxosResult = new Result(false, null);
    }

    return dummyPaxosResult;
  }
}
