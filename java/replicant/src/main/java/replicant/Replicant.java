package replicant;

import ch.qos.logback.classic.Logger;
import java.io.IOException;
import java.net.ServerSocket;
import kvstore.MemKVStore;
import log.Log;
import multipaxos.Configuration;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Replicant {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(Replicant.class);
    private ServerSocket server;
    private final long id;
    private final Log log;
    private final MultiPaxos multiPaxos;
    private final String ipPort;
    private final ExecutorService executorThread = Executors.newSingleThreadExecutor();
    private final ClientManager clientManager;


    public Replicant(Configuration config) {
        this.id = config.getId();
        this.log = new Log(new MemKVStore());
        this.multiPaxos = new MultiPaxos(log, config);
        this.ipPort = config.getPeers().get((int) id);
        clientManager = new ClientManager(id, config.getPeers().size(), multiPaxos);

    }

    public void start() {
        multiPaxos.start();
        startExecutorThread();
        startServer();
    }

    public void stop() {
        stopServer();
        stopExecutorThread();
        multiPaxos.stop();
    }

    private void startServer() {
        try {
            int port = Integer.parseInt(ipPort.substring(ipPort.indexOf(":") + 1)) + 1;
            this.server = new ServerSocket(port);
            logger.debug(id + " starting server at port " + port);
            acceptClient();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    private void stopServer() {
        try {
            server.close();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    private void startExecutorThread() {
        logger.debug(id + " starting executor thread");
        executorThread.submit(this::executorThread);
    }

    private void stopExecutorThread() {
        logger.debug(id + " stopping executor thread");
        log.stop();
        executorThread.shutdown();
    }

    private void executorThread() {
        while (true) {
            var r = log.execute();
            if (r == null) {
                break;
            }
            var id = r.getKey();
            var result = r.getValue();
            var client = clientManager.get(id);
            if (client != null) client.write(result.getValue());
        }
    }

    private void acceptClient(){
        while(true){
            try {
                var conn = server.accept();
                clientManager.start(conn);
            } catch (IOException e) {
                logger.warn(e.getMessage(),e);
                break;
            }
        }
    }

}
