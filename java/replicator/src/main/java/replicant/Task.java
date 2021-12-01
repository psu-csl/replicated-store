package replicant;

import command.Command;
import command.CommandResult;
import paxos.DummyPaxos;

import java.util.concurrent.Callable;

public class Task implements Callable<TCPResponse> {
    Command cmd;
    DummyPaxos paxos;

    public Task(Command cmd, DummyPaxos paxos) {
        this.cmd = cmd;
        this.paxos = paxos;
    }

    @Override
    public TCPResponse call() throws Exception {

        CommandResult commandResult = paxos.agreeAndExecute(cmd);
        TCPResponse response = new TCPResponse(commandResult.isSuccessful(), commandResult.getValue());
        return response;
        /* GET
        if (commandResult.getValue() != null && commandResult.isSuccessful()) {
            String content = "{\"value\":" + commandResult.getValue() + "}";
            return new Response(200, "application/json", content);
        }
        if (commandResult.isSuccessful()) {
            return new Response(200, "application/json", "{}");
        }
        return new Response(404, "application/json", "{}");

        */
    }
}