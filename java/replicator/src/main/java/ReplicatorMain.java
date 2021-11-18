import server.ReplicatorServer;

import java.io.IOException;

public class ReplicatorMain {
    public static void main(String [] args) throws IOException{
        ReplicatorServer server = new ReplicatorServer();
        server.startServer(8080);
        System.out.println("Server ready at 8080 and waiting....");

    }
}
