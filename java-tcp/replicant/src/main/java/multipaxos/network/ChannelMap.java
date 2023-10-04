package multipaxos.network;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelMap {

  private final ReentrantLock mu = new ReentrantLock();
  private final HashMap<Long, BlockingQueue<String>> channels = new HashMap<>();

  public BlockingQueue<String> get(Long channelId) {
    mu.lock();
    BlockingQueue<String> channel = channels.get(channelId);
    channels.remove(channelId);
    mu.unlock();
    return channel;
  }

  public void put(Long channelId, BlockingQueue<String> channel) {
    mu.lock();
    channels.put(channelId, channel);
    mu.unlock();
  }

  public void delete(Long channelId) {
    mu.lock();
    channels.remove(channelId);
    mu.unlock();
  }

  public void clear() {
    mu.lock();
    channels.clear();
    mu.unlock();
  }

}
