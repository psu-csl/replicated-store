package replicant;

import io.netty.channel.Channel;

public class Client {

    private Channel channel;
    public Client(Channel channel) {
        this.channel = channel;
    }

    public void write(String response) {
        this.channel.writeAndFlush(response+"\n");
    }
}
