package kvstore;

import command.Command;
import command.Command.CommandType;
import command.Result;
import java.util.concurrent.ConcurrentHashMap;

public class MemStore implements KVStore {

  private static final String kKeyNotFound = "key not found";
  private static final String kEmpty = "";
  private final ConcurrentHashMap<String, String> store;

  public MemStore() {
    this.store = new ConcurrentHashMap<>();
  }

  public String get(String key) {
    return store.get(key);
  }

  public boolean put(String key, String value) {
    store.put(key, value);
    // ?? Dummy implementation always inserts key with value
    // when it fails throw exception
    return true;
  }

  public boolean del(String key) {
    String value = store.remove(key);
    // key doesn't exist in store
    return value != null;
  }

  @Override
  public Result execute(Command cmd) {
    CommandType cmdType = cmd.getCommandType();
    if (cmdType == CommandType.kGet) {
      String r = get(cmd.getKey());
      if (r == null) {
        return new Result(false, kKeyNotFound);
      } else {
        return new Result(true, r);
      }
    } else if (cmdType == CommandType.kPut) {
      put(cmd.getKey(), cmd.getValue());
      return new Result(true, kEmpty);
    } else if (cmdType == CommandType.kDel) {
      return new Result(true, kEmpty);
    } else {
      // default action if command type is not matched
      return new Result(false, kKeyNotFound);
    }
  }
}

