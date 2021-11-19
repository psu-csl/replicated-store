package server;

public class KVRequest {
    public String value;
    public String key;

    @Override
    public String toString() {
        return "{\"key\" : " + key + "\n\"value\" : " + value + "\n}";
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
