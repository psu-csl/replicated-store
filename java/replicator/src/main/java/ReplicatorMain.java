public class ReplicatorMain {

  public static void main(String[] args) {

    /*MemStore kvStore = new MemStore();
    DummyPaxos paxos = new DummyPaxos(kvStore);
    int tpSize = 10;
    */

    /* ThreadPoolExecutor threadPool = new ThreadPoolExecutor(tpSize, tpSize, 50000L,
        TimeUnit.MILLISECONDS,

    new LinkedBlockingQueue<>());
    */
    //Replicant server = new Replicant(8888, paxos, kvStore, threadPool);
    // ReplicantTCP server = new ReplicantTCP(8888, paxos, kvStore, threadPool);
    //server.startServer(8888, kvStore);

    // System.out.println("Server ready at 8888 and waiting....");
    // Log log = new Log();
    // System.out.println(log.getLastIndex());
  }
}
