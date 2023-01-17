package replicant;

import command.Command;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;

import java.io.*;
import java.net.Socket;

public class Client implements Runnable {

    private final long id;
    private final Socket socket;
    private final MultiPaxos multiPaxos;
    private final ClientManager manager;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public Client(long id, Socket socket, MultiPaxos multiPaxos, ClientManager manager) {
        this.id = id;
        this.socket = socket;
        this.multiPaxos = multiPaxos;
        this.manager = manager;
        try {
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.writer = new PrintWriter(socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Command parse(String request) {
        if (request == null) {
            return null;
        }
        String[] tokens = request.split(" ");//"\\s+");
        String command = tokens[0];
        String key = tokens[1];
        Command res = new Command();
        res.setKey(key);
        if ("get".equals(command)) {
            res.setCommandType(Command.CommandType.Get);
        } else if ("del".equals(command)) {
            res.setCommandType(Command.CommandType.Del);
        } else if ("put".equals(command)) {
            res.setCommandType(Command.CommandType.Put);
            String value = tokens[2];
            if (value == null) {
                return null;
            }
            res.setValue(value);
        } else {
            return null;
        }
        return res;
    }

    public void start() {
        new Thread(this).start();
    }

    public void stop() {
        try {
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void run() {
        while (true) {
            String request;
            try {
                request = reader.readLine();
            } catch (IOException e) {
                manager.stop(id);
                return;
            }
            var command = parse(request);
            if (command != null) {
                var r = multiPaxos.replicate(command, id);
                if (r.type == MultiPaxosResultType.kOk) {
                    continue;
                } else if (r.type == MultiPaxosResultType.kRetry) {
                    write("retry");
                } else {
                    assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
                    write("leader is ...");
                }
            } else {
                write("bad command");
            }
        }
    }

    public void write(String response) {
        writer.write(response + "\n");
        writer.flush();
    }
}
