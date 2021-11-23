import kvstore.KVStore;
import paxos.DummyPaxos;
import replicant.Replicant;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReplicatorMain {
    public static void main(String[] args) throws IOException {

        KVStore kvStore = new KVStore();
        DummyPaxos paxos = new DummyPaxos(kvStore);
        int tpSize = 10;
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(tpSize, tpSize, 50000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        Replicant server = new Replicant(8888, paxos, kvStore, threadPool);
        //server.startServer(8888, kvStore);
        System.out.println("Server ready at 8888 and waiting....");

    }
}
