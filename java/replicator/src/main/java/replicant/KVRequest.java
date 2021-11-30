package replicant;

public class KVRequest {
    public String Value;
    public String Key;
    public String CommandType;

    @Override
    public String toString() {
        return "{\"Key\" : " + Key + "\n\"Value\" : " + Value + "\n\"CommandType :\"" + CommandType + "\n}";
    }

    public String getKey() {
        return Key;
    }

    public String getValue() {
        return Value;
    }

    public String getCommandType() {
        return CommandType;
    }
}
