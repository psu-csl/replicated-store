package kvstore;

import command.Command;
import command.KVResult;
import multipaxos.Configuration;

public interface KVStore {

  static KVStore createStore(Configuration config) {
    if (config.isStore()) {
      return new RocksDBStore(config.getRocksDBPath());
    } else {
      return new MemKVStore();
    }
  }

  String get(String key);

  boolean put(String key, String value);

  boolean del(String key);

  KVResult execute(Command cmd);
}
