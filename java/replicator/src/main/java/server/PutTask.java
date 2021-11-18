package server;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class PutTask implements Callable {

    String value;
    String key;
    ConcurrentHashMap<String, String> kvStore;

    public PutTask(ConcurrentHashMap<String, String> kvStore, String key, String value) {
        this.key = key;
        this.value = value;
        this.kvStore = kvStore;
    }

    @Override
    public Object call() throws Exception {
        return kvStore.put(key, value);

    }
}
