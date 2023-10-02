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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import multipaxos.network.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;

public class TcpLink {

  private final String address;
  private BlockingDeque<String> requestChan;
  private PrintWriter writer;
  private BufferedReader reader;
  private ExecutorService incomingThread = Executors.newSingleThreadExecutor();
  private ExecutorService outgoingThread = Executors.newSingleThreadExecutor();
  private final ReentrantLock mu;
  private final Condition cv;
  private AtomicBoolean isConnected;

  public TcpLink(String addr, ChannelMap channels) {
    this.address = addr;
    requestChan = new LinkedBlockingDeque<>();
    this.mu = new ReentrantLock();
    this.cv = mu.newCondition();
    isConnected = new AtomicBoolean(false);
    Connect();
    incomingThread.submit(() -> handleIncomingResponses(reader, channels));
    outgoingThread.submit(() -> handleOutgoingRequests(writer, requestChan));
  }

  private boolean Connect() {
    try {
      String[] address = this.address.split(":");
      int port = Integer.parseInt(address[1]);
      Socket socket = new Socket(address[0], port);
      writer = new PrintWriter(socket.getOutputStream(), true);
      reader = new BufferedReader(
          new InputStreamReader(socket.getInputStream()));
      mu.lock();
      isConnected = new AtomicBoolean(true);
      cv.signal();
      mu.unlock();
    } catch (IOException e) {
      return false;
    }
    return true;
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
        if (isConnected.get() || (!isConnected.get() && Connect())) {
          request = request + "\n";
          writer.print(request);
          writer.flush();
        }
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
        mu.lock();
        while(!isConnected.get()) {
          cv.await();
        }
        mu.unlock();
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
    mu.lock();
    isConnected = new AtomicBoolean(true);
    cv.signal();
    mu.unlock();
    requestChan.add("EOF");
    incomingThread.shutdown();
    outgoingThread.shutdown();
  }

}
