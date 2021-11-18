import server.ReplicatorServer;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicatorMain {
    public static void main(String[] args) throws IOException {
        ReplicatorServer server = new ReplicatorServer();
        ConcurrentHashMap<String, String> kvStore = new ConcurrentHashMap<>();
        server.startServer(8080, kvStore);
        System.out.println("Server ready at 8080 and waiting....");

    }
}
