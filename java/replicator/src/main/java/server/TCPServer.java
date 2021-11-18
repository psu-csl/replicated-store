package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TCPServer implements Cloneable, Runnable {

    Thread runner = null;
    ServerSocket server = null;
    Socket data = null;
    private boolean done = false;
    // TODO : this concurrent map acts as dummy paxos and kvstore
    ConcurrentHashMap<String, String> kvStore = null;
    ThreadPoolExecutor threadPool = null;

    // thread pool size
    // TODO : make this and other parameters tunable
    int tpSize = 10;

    // TODO : later reply concurrent map by KVStore context
    public synchronized void startServer(int port, ConcurrentHashMap<String, String> kvStore) throws IOException {
        if (runner == null) {
            server = new ServerSocket(port);
            runner = new Thread(this);
            // currently, both core and max size are set to tpSize
            // but can be adjusted to grow and shrink dynamically
            // from core # -> max # depending on the queue congestion
            threadPool = new ThreadPoolExecutor(tpSize, tpSize, 50000L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
            this.kvStore = kvStore;
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
