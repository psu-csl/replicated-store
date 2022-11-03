package replicant;

import multipaxos.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static util.TestUtil.makeConfig;

class ReplicantTest {

    @Test
    @Disabled
    void start() throws InterruptedException {
        Configuration config = makeConfig(0, 3);
        Replicant replicant = new Replicant(config);
        ExecutorService t0 = Executors.newSingleThreadExecutor();
        t0.submit(replicant::start);
        Thread.sleep(2000);
        replicant.stop();
    }
}