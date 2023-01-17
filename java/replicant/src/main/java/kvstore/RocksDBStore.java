package kvstore;

import ch.qos.logback.classic.Logger;
import command.Command;
import command.Command.CommandType;
import command.KVResult;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

public class RocksDBStore implements KVStore{

  public static final String kKeyNotFound = "key not found";
  public static final String kEmpty = "";
  private static final Logger logger = (Logger) LoggerFactory.getLogger(RocksDBStore.class);

  private RocksDB db;

  public RocksDBStore(String path){
    try {
      this.db = RocksDB.open(path);
    } catch (RocksDBException e) {
      logger.error("couldn't open the db connection ",e);
    }
  }
  @Override
  public String get(String key) {
    try {
      var result = db.get(key.getBytes());
      if(result!=null)
        return new String(result);
    } catch (RocksDBException e) {
      logger.error("error happened in get ",e);
    }
    return null;
  }

  @Override
  public boolean put(String key, String value) {
    try {
      db.put(key.getBytes(), value.getBytes());
      return true;
    } catch (RocksDBException e) {
      logger.error("error happened in put ",e);
      return false;
    }
  }

  @Override
  public boolean del(String key) {
    try {
      db.delete(key.getBytes());
      return true;
    } catch (RocksDBException e) {
      logger.error("error happened in delete ",e);
      return false;
    }
  }

  @Override
  public KVResult execute(Command cmd) {
    CommandType cmdType = cmd.getCommandType();
    if(cmdType == CommandType.Get){
      String r = get(cmd.getKey());
      if(r==null){
        return new KVResult(false, kKeyNotFound);
      }else{
        return new KVResult(true, r);
      }
    }else if(cmdType == CommandType.Put){
      put(cmd.getKey(), cmd.getValue());
      return new KVResult(true, kEmpty);
    }else if(cmdType == CommandType.Del && del(cmd.getKey())){
      return  new KVResult(true, kEmpty);
    }else{
      return new KVResult(false, kKeyNotFound);
    }
  }
}
