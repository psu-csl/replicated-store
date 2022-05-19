package kvstore;

import static kvstore.MemKVStore.kEmpty;
import static kvstore.MemKVStore.kKeyNotFound;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import command.Command;
import command.Command.CommandType;
import command.Result;
import org.junit.jupiter.api.Test;

class MemKVStoreTest {

  private static final String key1 = "foo";
  private static final String val1 = "bar";
  private static final String key2 = "baz";
  private static final String val2 = "quux";

  @Test
  void getPutDel() {
    MemKVStore store = new MemKVStore();

    assertNull(store.get(key1));

    assertFalse(store.del(key1));

    assertTrue(store.put(key1, val1));
    assertEquals(val1, store.get(key1));

    assertTrue(store.put(key2, val2));
    assertEquals(val2, store.get((key2)));

    assertTrue(store.put(key1, val2));
    assertEquals(val2, store.get(key1));
    assertEquals(val2, store.get(key2));

    assertTrue(store.del(key1));
    assertNull(store.get(key1));
    assertEquals(val2, store.get(key2));

    assertTrue(store.del(key2));
    assertNull(store.get(key1));
    assertNull(store.get(key2));
  }

  @Test
  void execute() {
    MemKVStore store = new MemKVStore();

    {
      Command get = new Command(CommandType.kGet, key1, "");
      Result r = store.execute(get);
      assertTrue(!r.isOk() && r.getValue().equals(kKeyNotFound));
    }
    {
      Command del = new Command(CommandType.kDel, key1, "");
      Result r = store.execute(del);
      assertTrue(!r.isOk() && r.getValue().equals(kKeyNotFound));
    }
    {

      Command put = new Command(CommandType.kPut, key1, val1);
      Result r1 = store.execute((put));
      assertTrue(r1.isOk() && r1.getValue().equals(kEmpty));

      Command get = new Command(CommandType.kGet, key1, "");
      Result r2 = store.execute((get));
      assertTrue(r2.isOk() && r2.getValue().equals(val1));
    }
    {
      Command put = new Command(CommandType.kPut, key2, val2);
      Result r1 = store.execute(put);
      assertTrue(r1.isOk() && r1.getValue().equals(kEmpty));

      Command get = new Command(CommandType.kGet, key2, "");
      Result r2 = store.execute(get);
      assertTrue(r2.isOk() && r2.getValue().equals(val2));
    }
    {
      Command put = new Command(CommandType.kPut, key1, val2);
      Result r1 = store.execute(put);
      assertTrue(r1.isOk() && r1.getValue().equals(kEmpty));

      Command get1 = new Command(CommandType.kGet, key1, "");
      Result r2 = store.execute(get1);
      assertTrue(r2.isOk() && r2.getValue().equals(val2));

      Command get2 = new Command(CommandType.kGet, key2, "");
      Result r3 = store.execute(get2);
      assertTrue(r3.isOk() && r3.getValue().equals(val2));
    }
    {
      Command del = new Command(CommandType.kDel, key1, "");
      Result r1 = store.execute(del);
      assertTrue(r1.isOk() && r1.getValue().equals(kEmpty));

      Command get1 = new Command(CommandType.kGet, key1, "");
      Result r2 = store.execute(get1);
      assertFalse(r2.isOk() && r2.getValue().equals(kKeyNotFound));

      Command get2 = new Command(CommandType.kGet, key2, "");
      Result r3 = store.execute(get2);
      assertTrue(r3.isOk() && r3.getValue().equals(val2));
    }
  }

}