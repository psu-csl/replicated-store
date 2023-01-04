package kvstore;

import command.Command;
import command.KVResult;
import multipaxos.Configuration;

public interface KVStore {

  static KVStore createStore(Configuration config) {
    if (config.getStore().equals("rocksdb")) {
      return new RocksDBStore(config.getRocksDBPath());
    } else if (config.getStore().equals("mem")) {
      return new MemKVStore();
    } else {
      assert false;
      return null;
    }
  }

  String get(String key);

  boolean put(String key, String value);

  boolean del(String key);

  KVResult execute(Command cmd);
}
