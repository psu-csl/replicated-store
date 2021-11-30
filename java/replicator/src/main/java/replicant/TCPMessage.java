package replicant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TCPMessage {
    private final String msg;

    public TCPMessage(InputStream inputStream) throws IOException {

        BufferedReader in = new BufferedReader(new InputStreamReader(
                inputStream));
        try {
            //String requestLine = in.readLine();
            char[] buff = new char[1024];

            in.read(buff, 0, buff.length);
            String requestLine = new String(buff);
            System.out.println(requestLine);
            if (requestLine == null) {
                System.err.println("request is null");
                throw new IOException();
            }
            this.msg = requestLine;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException();
        }
    }

    public String getContent() {
        return this.msg;
    }
}
