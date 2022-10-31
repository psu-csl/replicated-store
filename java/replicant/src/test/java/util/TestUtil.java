package util;

import command.Command;
import command.Command.CommandType;
import java.util.ArrayList;
import java.util.List;
import log.Instance;
import log.Instance.InstanceState;
import multipaxos.Configuration;

public class TestUtil {


  public static Instance makeInstance(long ballot, long index) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command());
  }

  public static Instance makeInstance(long ballot, long index, CommandType type) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command(type, "", ""));
  }

  public static Instance makeInstance(long ballot, long index, InstanceState state,
      CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }

  public static Instance makeInstance(long ballot, long index, InstanceState state) {
    return new Instance(ballot, index, 0, state, new Command());
  }

  public static Configuration makeConfig(long id, int numPeers) {
    assert (id < numPeers);
    Configuration config = new Configuration();
    config.setId(id);
    config.setCommitInterval(300);
    config.setThreadPoolSize(8);
    List<String> peers = new ArrayList<>();
    for (int i = 0; i < numPeers; i++) {
      peers.add("127.0.0.1:1" + i + "000");
    }
    config.setPeers(peers);
    return config;
  }
}
