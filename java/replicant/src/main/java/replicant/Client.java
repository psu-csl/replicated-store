package replicant;

import ch.qos.logback.classic.Logger;
import command.Command;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;
import org.slf4j.LoggerFactory;

public class Client {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(Client.class);

  private final long id;

  private final BufferedReader reader;
  private final BufferedWriter writer;
  private final Socket socket;
  private final MultiPaxos multiPaxos;
  private final ClientManager manager;
  private Thread t;

  public Client(long id, Socket socket, MultiPaxos multiPaxos, ClientManager manager)
      throws IOException {
    this.id = id;
    this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    this.socket = socket;
    this.multiPaxos = multiPaxos;
    this.manager = manager;
  }

  public static Command parse(String request) {
    String[] tokens = request.split(" ");//"\\s+");
    String commandType = tokens[0];
    String key = tokens[1];
    Command command = new Command();
    command.setKey(key);
    if ("get".equals(commandType)) {
      command.setCommandType(Command.CommandType.Get);
    } else if ("del".equals(commandType)) {
      command.setCommandType(Command.CommandType.Del);
    } else if ("put".equals(commandType)) {
      command.setCommandType(Command.CommandType.Put);
      String value = tokens[2];
      if (value == null) {
        return null;
      }
      command.setValue(value);
    } else {
      return null;
    }
    return command;
  }

  public void start() {
    t = new Thread(this::read);
    t.start();
  }

  public void stop() {
    try {
      socket.close();
      t.join();
    } catch (IOException e) {
      logger.warn(e.getMessage(), e);
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }

  public void read() {
    while (!socket.isClosed() && socket.isConnected()) {
      String msg = null;
      try {
        msg = reader.readLine();
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
        manager.stop(this.id);
      }
      if(msg == null){
        break;
      }
      var command = parse(msg);
      if (command != null) {
        var r = multiPaxos.replicate(command, id);
        if (r.type == MultiPaxosResultType.kOk) {
          continue;
        }
        if (r.type == MultiPaxosResultType.kRetry) {
          write("retry");
        } else {
          assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
          write("leader is ...");
        }
      } else {
        write("bad command");
      }
    }
    if (socket.isClosed() || !socket.isConnected()) {
          manager.stop(id);
      }
  }

  public void write(String response) {
    try {
      writer.write(response + "\n");
      writer.flush();
    } catch (IOException e) {
      logger.warn(e.getMessage(), e);
//            e.printStackTrace();
    }
  }
}
