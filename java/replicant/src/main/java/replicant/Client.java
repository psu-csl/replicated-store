package replicant;

import command.Command;
import io.netty.channel.Channel;
import multipaxos.MultiPaxos;
import multipaxos.MultiPaxosResultType;

public class Client {

    private final long id;
    private final Channel socket;
    private final MultiPaxos multiPaxos;

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
    public Client(long id,Channel socket, MultiPaxos multiPaxos) {
        this.id = id;
        this.socket = socket;
        this.multiPaxos = multiPaxos;
    }
        public void read(String msg) {
        var command = parse(msg);
        var r = multiPaxos.replicate(command, id);
        if (r.type == MultiPaxosResultType.kOk) {
            socket.flush();
        } else if (r.type == MultiPaxosResultType.kRetry) {
            socket.writeAndFlush("retry\n");
        } else {
            assert r.type == MultiPaxosResultType.kSomeoneElseLeader;
            socket.writeAndFlush("leader is ...\n");
        }
    }

    public void write(String response) {
        socket.writeAndFlush(response+"\n");
    }
}
