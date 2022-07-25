package paxos;

import command.Command;
import command.Command.CommandType;
import command.KVResult;
import kvstore.MemKVStore;

public class DummyPaxos {

  private final MemKVStore memStore;

  public DummyPaxos(MemKVStore memKVStore) {
    this.memStore = memKVStore;
  }

  public KVResult agreeAndExecute(Command cmd) {
    CommandType cmdName = cmd.getCommandType();
    boolean isSuccess;
    KVResult dummyPaxosResult;
    switch (cmdName) {
      case kGet -> {
        String value = memStore.get(cmd.getKey());
        dummyPaxosResult = new KVResult(true, value);
      }
      case kPut -> {
        isSuccess = memStore.put(cmd.getKey(), cmd.getValue());
        dummyPaxosResult = new KVResult(isSuccess, null);
      }
      case kDel -> {
        isSuccess = memStore.del(cmd.getKey());
        dummyPaxosResult = new KVResult(isSuccess, null);
      }
      default -> dummyPaxosResult = new KVResult(false, null);
    }

    return dummyPaxosResult;
  }
}
