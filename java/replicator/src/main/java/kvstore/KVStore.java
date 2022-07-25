package kvstore;

import command.Command;
import command.KVResult;

public interface KVStore {

  String get(String key);

  boolean put(String key, String value);

  boolean del(String key);

  KVResult execute(Command cmd);
}
