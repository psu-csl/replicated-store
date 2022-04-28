package replicant;

import command.Command;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import kvstore.KVStore;
import org.codehaus.jackson.map.ObjectMapper;
import paxos.DummyPaxos;

public class Replicant extends TCPServer {

  public Replicant(int port, DummyPaxos paxos, KVStore kvStore, ThreadPoolExecutor threadPool) {

    try {
      startServer(port, paxos, kvStore, threadPool);
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
        HTTPRequest request = new HTTPRequest(in);
        //System.out.println(request);

        // deserialize the content of request
        String json = request.getContent();
        System.out.println("Content : " + json);
        ObjectMapper objectMapper = new ObjectMapper();
        KVRequest kvRequest = objectMapper.readValue(json, KVRequest.class);
        String key = kvRequest.getKey();
        String value = kvRequest.getValue();

        //  send requests to paxos to be handled over thread pool
        String method = request.getMethod();

        Command cmd = new Command(kvRequest.getKey(), kvRequest.getValue(), request.getMethod());
        Future<TCPResponse> future = threadPool.submit(new Task(cmd, paxos));
        TCPResponse response = future.get();
        System.out.println(response);

        PrintWriter out = new PrintWriter(data.getOutputStream());
        out.print(response.toString());
        out.flush();
        out.close();
        in.close();

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
