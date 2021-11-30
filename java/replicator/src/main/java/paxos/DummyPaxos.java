package paxos;

import command.Command;
import command.CommandResult;
import kvstore.KVStore;

public class DummyPaxos {
    private final KVStore kvStore;

    public DummyPaxos(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    public CommandResult agreeAndExecute(Command cmd) {
        String cmdName = cmd.getCommandType();
        boolean isSuccess;
        CommandResult dummyPaxosResult;
        switch (cmdName) {
            case "Get":
                String value = kvStore.get(cmd.getKey());
                dummyPaxosResult = new CommandResult(true, value);
                break;
            case "Put":
                isSuccess = kvStore.put(cmd.getKey(), cmd.getValue());
                dummyPaxosResult = new CommandResult(isSuccess, null);
                break;
            case "Del":
                isSuccess = kvStore.del(cmd.getKey());
                dummyPaxosResult = new CommandResult(isSuccess, null);
                break;
            default:
                dummyPaxosResult = new CommandResult(false, null);
        }

        return dummyPaxosResult;
    }
}
