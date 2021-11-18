package server;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class GetTask implements Callable {

    String key;
    ConcurrentHashMap<String, String> kvStore;

    public GetTask(ConcurrentHashMap<String, String> kvStore, String key) {
        this.key = key;
        this.kvStore = kvStore;
    }


    @Override
    public String call() throws Exception {
        return kvStore.get(key);
    }
}
