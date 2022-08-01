import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kvstore.KVStore;
import kvstore.MemKVStore;
import paxos.DummyPaxos;
import replicant.ReplicantTCP;

public class ReplicatorMain {

  public static void main(String[] args) throws IOException {

    KVStore kvStore = new MemKVStore();
    DummyPaxos paxos = new DummyPaxos(kvStore);
    int tpSize = 10;

    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(tpSize, tpSize, 50000L,
        TimeUnit.MILLISECONDS,

        new LinkedBlockingQueue<>());
    //Replicant server = new Replicant(8888, paxos, kvStore, threadPool);
    ReplicantTCP server = new ReplicantTCP(8888, paxos, kvStore, threadPool);
    server.startServer(8888, paxos, kvStore, threadPool);

    System.out.println("Server ready at 8888 and waiting....");
    // Log log = new Log();
    //System.out.println(log.getLastIndex());
  }
}
