package replicant;

import ch.qos.logback.classic.Logger;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import multipaxos.MultiPaxos;
import org.slf4j.LoggerFactory;

public class ClientManager{

  private static final Logger logger = (Logger) LoggerFactory.getLogger(ClientManager.class);
  private long nextClientId;
  private final long numPeers;
  private final MultiPaxos multiPaxos;
  private final ConcurrentHashMap<Long, Client> clients = new ConcurrentHashMap<>();

  public ClientManager(long id, long numPeers, MultiPaxos multiPaxos) {
    this.nextClientId = id;
    this.numPeers = numPeers;
    this.multiPaxos = multiPaxos;
  }

  private long nextClientId() {
    var id = nextClientId;
    nextClientId += numPeers;
    return id;
  }

  public void start(Socket socket){
    var id = nextClientId();
    try {
      var client = new Client(id, socket, multiPaxos, this);
      clients.put(id,client);
      logger.info("client_manager started client "+id);
      client.start();
    } catch (IOException e) {
      logger.warn(e.getMessage());
//      e.printStackTrace();
    }
  }
  public Client get(Long clientId) {
    return clients.get(clientId);
  }
  public void stop(Long id){
    logger.info("client_manager stopped client " + id);
    var client = clients.get(id);
    if(client == null){
      logger.warn("no client to stop " + id);
      return;
    }
    client.stop();
    clients.remove(id);
  }

  public void stopAll(){
    clients.forEach((id,client)->{
      logger.info("client_manager stopping all clients " + id);
      client.stop();
      clients.remove(id);
    });
  }
}
