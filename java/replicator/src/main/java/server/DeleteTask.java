package server;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class DeleteTask implements Callable {

    String key;
    ConcurrentHashMap<String, String> kvStore;

    public DeleteTask(ConcurrentHashMap<String, String> kvStore, String key) {
        this.kvStore = kvStore;
        this.key = key;
    }

    @Override
    public Object call() throws Exception {
        return kvStore.remove(key);
    }
}
