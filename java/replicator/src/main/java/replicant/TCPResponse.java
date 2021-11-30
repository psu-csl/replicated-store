package replicant;

public class TCPResponse {
    public boolean IsSuccess;
    public String Value;

    public TCPResponse(boolean successful, String value) {
        this.IsSuccess = successful;
        this.Value = value;
    }

    @Override
    public String toString() {
        return "{\"IsSuccess\":" + this.IsSuccess + ",\"Value\":\"" + this.Value + "\"}";
    }
}
