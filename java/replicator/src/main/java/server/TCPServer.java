package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer implements Cloneable, Runnable {

    Thread runner = null;
    ServerSocket server = null;
    Socket data = null;
    private boolean done = false;

    public synchronized void startServer(int port) throws IOException {
        if (runner == null) {
            server = new ServerSocket(port);
            runner = new Thread(this);
            runner.start();
        }
    }

    public synchronized void stopServer() {
        done = true;
        runner.interrupt();
    }

    public synchronized boolean getDone() {
        return done;
    }

    public void run() {
        if (server != null) {
            while (!getDone()) {
                try {
                    Socket dataSocket = server.accept();
                    TCPServer newConn = (TCPServer) clone();

                    newConn.server = null;
                    newConn.data = dataSocket;
                    newConn.runner = new Thread(newConn);
                    newConn.runner.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            run(data);
        }
    }

    public void run(Socket data) {
        // implement in subclass
    }
}
