package kvstore;

import java.util.concurrent.ConcurrentHashMap;

public class KVStore {
    private final ConcurrentHashMap<String, String> store;

    public KVStore() {
        this.store = new ConcurrentHashMap<String, String>();
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
}

