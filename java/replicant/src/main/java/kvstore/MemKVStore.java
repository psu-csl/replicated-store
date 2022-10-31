package kvstore;

import command.Command;
import command.Command.CommandType;
import command.KVResult;

import java.util.HashMap;
public class MemKVStore implements KVStore {

  public static final String kKeyNotFound = "key not found";
  public static final String kEmpty = "";
  private final HashMap<String, String> store;

  public MemKVStore() {
    this.store = new HashMap<>();
  }

  public String get(String key) {
    return store.get(key);
  }

  public boolean put(String key, String value) {
    store.put(key, value);
    return true;
  }

  public boolean del(String key) {
    String value = store.remove(key);
    return value != null;
  }

  @Override
  public KVResult execute(Command cmd) {
    CommandType cmdType = cmd.getCommandType();
    if (cmdType == CommandType.Get) {
      String r = get(cmd.getKey());
      if (r == null) {
        return new KVResult(false, kKeyNotFound);
      } else {
        return new KVResult(true, r);
      }
    } else if (cmdType == CommandType.Put) {
      put(cmd.getKey(), cmd.getValue());
      return new KVResult(true, kEmpty);
    } else if (cmdType == CommandType.Del && del(cmd.getKey())) {
      return new KVResult(true, kEmpty);
    } else {
      return new KVResult(false, kKeyNotFound);
    }
  }
}

