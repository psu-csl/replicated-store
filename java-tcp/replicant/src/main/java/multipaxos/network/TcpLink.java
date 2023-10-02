package multipaxos.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import multipaxos.network.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;

public class TcpLink {

  private BlockingDeque<String> requestChan;
  private ExecutorService incomingThread = Executors.newSingleThreadExecutor();
  private ExecutorService outgoingThread = Executors.newSingleThreadExecutor();

  public TcpLink(String addr, ChannelMap channels) {
    String[] address = addr.split(":");
    int port = Integer.parseInt(address[1]);
    Socket socket = null;
    requestChan = new LinkedBlockingDeque<>();
    while (true) {
      try {
        socket = new Socket(address[0], port);
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(socket.getInputStream()));
        incomingThread.submit(() -> handleIncomingResponses(reader, channels));
        outgoingThread.submit(() -> handleOutgoingRequests(writer, requestChan));
      } catch (IOException e) {
        continue;
      }
      break;
    }
  }

  public void sendAwaitResponse(MessageType type, long channelId, String msg) {
    ObjectMapper mapper = new ObjectMapper();
    Message request = new Message(type, channelId, msg);
    String requestString;
    try {
      requestString = mapper.writeValueAsString(request);
      requestChan.put(requestString);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void handleOutgoingRequests(PrintWriter writer,
      BlockingDeque<String> requestChan) {
    while (true) {
      try {
        String request = requestChan.take();
        if (request.equals("EOF")) {
          return;
        }
        request = request + "\n";
        writer.print(request);
        writer.flush();
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public void handleIncomingResponses(BufferedReader reader,
      ChannelMap channels) {
    ObjectMapper mapper = new ObjectMapper();
    while (true) {
      try {
        String line = reader.readLine();
        var response = mapper.readValue(line, Message.class);
        var channel = channels.get(response.getChannelId());
        if (channel != null) {
          channel.put(response.getMsg());
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
    channels.clear();
  }

  public void stop() {
    requestChan.add("EOF");
    incomingThread.shutdown();
    outgoingThread.shutdown();
  }

}
