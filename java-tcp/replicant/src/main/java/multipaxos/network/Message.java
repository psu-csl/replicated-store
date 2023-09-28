package multipaxos.network;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class Message {

  @JsonProperty("type")
  private MessageType type;
  @JsonProperty("channelId")
  private long channelId;
  @JsonProperty("msg")
  private String msg;

  @JsonCreator
  public Message(@JsonProperty("type") MessageType type,
                 @JsonProperty("channelId") long channelId,
                 @JsonProperty("name") String msg) {
    this.type = type;
    this.channelId = channelId;
    this.msg = msg;
  }

  public MessageType getType() {
    return type;
  }

  public void setType(MessageType type) {
    this.type = type;
  }

  public long getChannelId() {
    return channelId;
  }

  public void setChannelId(long channelId) {
    this.channelId = channelId;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public enum MessageType {
    PREPAREREQUEST, PREPARERESPONSE, ACCEPTREQUEST,
    ACCEPTRESPONSE, COMMITREQUEST, COMMITRESPONSE;

//    private static Map<String, MessageType> map = new HashMap<>(6);
//
//    static {
//      map.put("PREPAREREQUEST", PREPAREREQUEST);
//      map.put("PREPARERESPONSE", PREPARERESPONSE);
//      map.put("ACCEPTREQUEST", ACCEPTREQUEST);
//      map.put("ACCEPTRESPONSE",  ACCEPTRESPONSE);
//      map.put("COMMITREQUEST", COMMITREQUEST);
//      map.put("COMMITRESPONSE", COMMITRESPONSE);
//    }
//
//    @JsonValue
//    public String toValue() {
//      for (var entry : map.entrySet()) {
//        if (entry.getValue() == this) {
//          return entry.getKey();
//        }
//      }
//      return null;
//    }
//
//    @JsonCreator
//    public static MessageType forValue(String value) {
//      return map.get(value);
//    }
  }

}
