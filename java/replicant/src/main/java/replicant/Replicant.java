package replicant;

import ch.qos.logback.classic.Logger;

import kvstore.KVStore;
import log.Log;
import multipaxos.Configuration;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Replicant {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(Replicant.class);
    private final long id;
    private final Log log;
    private final MultiPaxos multiPaxos;
    private final String ipPort;
    private final ExecutorService executorThread = Executors.newSingleThreadExecutor();
    private ServerSocket acceptor;
    private final ClientManager clientManager;


    public Replicant(Configuration config) {
        this.id = config.getId();
        this.log = new Log(KVStore.createStore(config));
        this.multiPaxos = new MultiPaxos(log, config);
        this.ipPort = config.getPeers().get((int) id);
        this.acceptor = null;
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
        int pos = ipPort.indexOf(":");
        assert (pos != -1);
        pos += 1;
        int port = Integer.parseInt(ipPort.substring(pos)) + 1;

        try {
            acceptor = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debug(id + " starting server at port " + port);
        acceptClient();
    }

    private void stopServer() {
       try {
           acceptor.close();
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
        clientManager.stopAll();
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

    private void acceptClient() {
        while (true) {
            try {
                Socket socket = acceptor.accept();
                clientManager.start(socket);
            } catch (IOException e) {
                logger.error(e.getMessage());
                break;
            }
        }
    }
}
