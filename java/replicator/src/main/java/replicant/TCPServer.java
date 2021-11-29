package replicant;

import kvstore.KVStore;
import paxos.DummyPaxos;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadPoolExecutor;

public class TCPServer implements Cloneable, Runnable {

    Thread runner = null;
    ServerSocket server = null;
    Socket data = null;
    private boolean done = false;
    DummyPaxos paxos;
    ThreadPoolExecutor threadPool = null;
    // TODO : this concurrent map acts as dummy paxos and kvstore
    private KVStore kvStore;

    // TODO : later reply concurrent map by KVStore context
    public synchronized void startServer(int port, DummyPaxos paxos, KVStore kvStore, ThreadPoolExecutor threadPool/*int port, ConcurrentHashMap<String, String> kvStore*/) throws IOException {
        if (runner == null) {
            server = new ServerSocket(port);
            runner = new Thread(this);
            // currently, both core and max size are set to tpSize
            // but can be adjusted to grow and shrink dynamically
            // from core # -> max # depending on the queue congestion
            this.threadPool = threadPool; /*n*/
            this.kvStore = kvStore;
            this.paxos = paxos;
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
