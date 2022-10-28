package replicant;

import static util.TestUtil.makeConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import paxos.Configuration;

class ReplicantTest {

  @Test
  void start() throws InterruptedException {
    Configuration config = makeConfig(0, 3);
    Replicant replicant = new Replicant(config);
    ExecutorService t0 = Executors.newSingleThreadExecutor();
    t0.submit(replicant::start);
    while (true) {
    }
    /*Thread.sleep(3000);
    replicant.stop();
    t0.shutdown();*/
  }
}