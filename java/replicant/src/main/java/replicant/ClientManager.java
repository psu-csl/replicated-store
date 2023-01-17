package replicant;

import ch.qos.logback.classic.Logger;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.AttributeKey;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Sharable
public class ClientManager {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientManager.class);
    private long nextId;
    private final long numPeers;
    private final MultiPaxos multiPaxos;
    private final AttributeKey<Long> clientIdAttrKey = AttributeKey.valueOf("ClientID");
    private final ReentrantLock mu = new ReentrantLock();
    private final HashMap<Long, Client> clients = new HashMap<>();

    public ClientManager(long id, long numPeers, MultiPaxos multiPaxos) {
        this.nextId = id;
        this.numPeers = numPeers;
        this.multiPaxos = multiPaxos;
    }

    private long nextClientId() {
        var id = nextId;
        nextId += numPeers;
        return id;
    }

    public void start(Socket socket) {
        var id = nextClientId();
        Client client = new Client(id, socket, multiPaxos, this);
        mu.lock();
        clients.put(id, client);
        mu.unlock();
        logger.debug("client_manager started client " + id);
        client.start();
    }

    public Client get(Long id) {
        mu.lock();
        try {
            return clients.get(id);
        } finally {
            mu.unlock();
        }
    }

    public void stop(Long id) {
        mu.lock();
        logger.debug("client_manager stopped client " + id);
        Client client = clients.get(id);
        assert (client != null);
        client.stop();
        clients.remove(id);
        mu.unlock();
    }

    public void stopAll() {
        mu.lock();
        for (Map.Entry<Long, Client> entry : clients.entrySet()) {
            Long id = entry.getKey();
            Client client = entry.getValue();
            logger.debug("client_manager stopping all clients " + id);
            client.stop();
            clients.remove(id);
        }
        mu.unlock();
    }
}
