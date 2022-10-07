import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import paxos.Configuration;
import replicantv2.Replicant;

public class ReplicatorMain {

  public static void main(String[] args) {

    if (args.length != 2) {
      System.err.println("Correct usage: [id] [path to config.json]");
      System.exit(1);
    }
    int id = Integer.parseInt(args[0]);
    String path = args[1];

    ObjectMapper objectMapper = new ObjectMapper();
    Configuration config = null;
    try {
      config = objectMapper.readValue(
          new File(path),
          Configuration.class);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      System.err.println("Couldn't parse config.json");
      System.exit(1);
    }
    assert id < config.getPeers().size();
    config.setId(id);

    var replicant = new Replicant(config);
    replicant.start();
  }
}
