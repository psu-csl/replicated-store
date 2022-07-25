package kvstore;

import command.Command;
import command.Command.CommandType;
import command.KVResult;
import java.util.concurrent.ConcurrentHashMap;

public class MemKVStore implements KVStore {

  public static final String kKeyNotFound = "key not found";
  public static final String kEmpty = "";
  private final ConcurrentHashMap<String, String> store;

  public MemKVStore() {
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
  public KVResult execute(Command cmd) {
    CommandType cmdType = cmd.getCommandType();
    if (cmdType == CommandType.kGet) {
      String r = get(cmd.getKey());
      if (r == null) {
        return new KVResult(false, kKeyNotFound);
      } else {
        return new KVResult(true, r);
      }
    } else if (cmdType == CommandType.kPut) {
      put(cmd.getKey(), cmd.getValue());
      return new KVResult(true, kEmpty);
    } else if (cmdType == CommandType.kDel && del(cmd.getKey())) {
      return new KVResult(true, kEmpty);
    } else {
      // default action if command type is not matched
      return new KVResult(false, kKeyNotFound);
    }
  }
}

