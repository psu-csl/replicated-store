package kvstore;

import command.Command;
import command.Result;

public interface KVStore {

  String get(String key);

  boolean put(String key, String value);

  boolean del(String key);

  Result execute(Command cmd);
}
