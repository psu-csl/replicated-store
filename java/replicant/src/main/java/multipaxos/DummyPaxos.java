package multipaxos;

import command.Command;
import command.Command.CommandType;
import command.KVResult;
import kvstore.KVStore;

public class DummyPaxos {

  private final KVStore memStore;

  public DummyPaxos(KVStore memKVStore) {
    this.memStore = memKVStore;
  }

  public KVResult agreeAndExecute(Command cmd) {
    CommandType cmdName = cmd.getCommandType();
    boolean isSuccess;
    KVResult dummyPaxosResult;
    switch (cmdName) {
      case Get -> {
        String value = memStore.get(cmd.getKey());
        dummyPaxosResult = new KVResult(true, value);
      }
      case Put -> {
        isSuccess = memStore.put(cmd.getKey(), cmd.getValue());
        dummyPaxosResult = new KVResult(isSuccess, null);
      }
      case Del -> {
        isSuccess = memStore.del(cmd.getKey());
        dummyPaxosResult = new KVResult(isSuccess, null);
      }
      default -> dummyPaxosResult = new KVResult(false, null);
    }

    return dummyPaxosResult;
  }
}
