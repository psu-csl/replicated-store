package replicant;

import command.Command;
import command.Command.CommandType;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import kvstore.MemStore;
import org.codehaus.jackson.map.ObjectMapper;
import paxos.DummyPaxos;

public class ReplicantTCP extends TCPServer {

  public ReplicantTCP(int port, DummyPaxos paxos, MemStore memStore,
      ThreadPoolExecutor threadPool) {

    try {
      startServer(port, paxos, memStore, threadPool);
      System.out.println("Successfully created a new replicant server!");
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Couldn't create a replicant server!");
    }
  }

  public void run(Socket data) {
    while (!data.isClosed() && data.isConnected()) {
      try {
        InputStream in = data.getInputStream();
        // create a request object from input stream
        TCPMessage msg = new TCPMessage(in);
        //System.out.println(request);

        // deserialize the content of request
        String json = msg.getContent();
        //System.out.println("Content : " + json);
        ObjectMapper objectMapper = new ObjectMapper();
        KVRequest kvRequest = objectMapper.readValue(json, KVRequest.class);
        String key = kvRequest.getKey();
        String value = kvRequest.getValue();

        //  send requests to paxos to be handled over thread pool
        CommandType method = kvRequest.getCommandType();

        Command cmd = new Command(kvRequest.getKey(), kvRequest.getValue(), method);
        Future<TCPResponse> future = threadPool.submit(new Task(cmd, paxos));
        TCPResponse response = future.get();
        System.out.println(response);

        PrintWriter out = new PrintWriter(data.getOutputStream());
        out.print(response);
        out.print("\n");
        out.flush();
        //  out.close();
        //  in.close();

      } catch (IOException | InterruptedException | ExecutionException e) {
        e.printStackTrace();
        try {
          data.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }

  }
}
