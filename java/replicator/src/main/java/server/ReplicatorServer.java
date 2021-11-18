package server;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ReplicatorServer extends TCPServer {

    public void run(Socket data) {
        try {
            InputStream in = data.getInputStream();
            // create a request object from input stream
            Request request = new Request(in);
            System.out.println(request);

            // create thread pool and send requests to paxos to be handled
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
