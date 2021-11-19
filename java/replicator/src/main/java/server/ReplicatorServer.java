package server;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ReplicatorServer extends TCPServer {

    public void run(Socket data) {
        try {
            InputStream in = data.getInputStream();
            // create a request object from input stream
            Request request = new Request(in);
            System.out.println(request);

            // deserialize the content of request
            String json = request.getContent();
            ObjectMapper objectMapper = new ObjectMapper();
            KVRequest kvRequest = objectMapper.readValue(json, KVRequest.class);
            String key = kvRequest.getKey();
            String value = kvRequest.getValue();

            //  send requests to paxos to be handled over thread pool
            String method = request.getMethod();
            switch (method) {
                case "GET":
                    Future<String> f = threadPool.submit(new GetTask(kvStore, key));
                    String result = f.get();
                    if (result != null) {
                        //
                        System.out.println("GET " + key + " : " + result);
                    } else {
                        System.out.println("Couldn't find key ->  " + key);
                    }
                    break;
                case "POST":
                    f = threadPool.submit(new PostTask(kvStore, key, value));
                    //System.out.println(f.toString());
                    // f can be used to further notify or get result from
                    // PostTask thread
                    break;
                case "DELETE":
                    f = threadPool.submit(new DeleteTask(kvStore, key));
                    // f can be used to further notify or get result from
                    // DeleteTask thread
                    break;
                default:
                    System.err.println("Undefined method! " + method);
                    break;
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
