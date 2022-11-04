package log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static util.TestUtil.makeInstance;

import command.Command.CommandType;
import command.KVResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import kvstore.MemKVStore;
import log.Instance.InstanceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogTest {

  private MemKVStore store_;
  private Log log;


  @BeforeEach
  void SetUp() {
    store_ = new MemKVStore();
    log = new Log(store_);
  }

  @Test
  void constructor() {
    assertEquals(log.getLastExecuted(), 0);
    assertEquals(log.getGlobalLastExecuted(), 0);
    assertFalse(log.isExecutable());
    assertNull(log.at(0L));
    assertNull(log.at(-1L));
    assertNull(log.at(3L));
  }

  @Test
  void insert() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, makeInstance(ballot, index, CommandType.Put)));
    assertEquals(CommandType.Put, log.get(index).getInstance().getCommand().getCommandType());
    assertFalse(Log.insert(log, makeInstance(ballot, index, CommandType.Put)));
  }

  @Test
  void insertUpdateInProgress() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, makeInstance(ballot, index, CommandType.Put)));
    assertEquals(CommandType.Put, log.get(index).getInstance().getCommand().getCommandType());
    assertFalse(Log.insert(log, makeInstance(ballot, index, CommandType.Put)));
  }

  @Test
  void insertUpdateCommitted() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(
        Log.insert(log, makeInstance(ballot, index, InstanceState.kCommitted, CommandType.Put)));
    assertFalse(
        Log.insert(log, makeInstance(ballot, index, InstanceState.kInProgress, CommandType.Put)));

  }

  @Test
  void insertStale() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, makeInstance(ballot, index, CommandType.Put)));
    assertEquals(CommandType.Put, log.get(index).getInstance().getCommand().getCommandType());
    assertFalse(Log.insert(log, makeInstance(0, index, CommandType.Del)));
    assertEquals(CommandType.Put, log.get(index).getInstance().getCommand().getCommandType());
  }

  @Test
  void insertCase2Committed() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1;
    var inst1 = makeInstance(0, index, InstanceState.kCommitted, CommandType.Put);
    var inst2 = makeInstance(0, index, InstanceState.kInProgress, CommandType.Del);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void insertCase2Executed() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1;
    var inst1 = makeInstance(0, index, InstanceState.kExecuted, CommandType.Put);
    var inst2 = makeInstance(0, index, InstanceState.kInProgress, CommandType.Del);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void insertCase3() {
    HashMap<Long, InstanceWithCv> log = new HashMap<>();
    long index = 1;
    var inst1 = makeInstance(0, index, InstanceState.kInProgress, CommandType.Put);
    var inst2 = makeInstance(0, index, InstanceState.kInProgress, CommandType.Del);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case3", thrown.getMessage());
  }

  @Test
  void append() {
    log.append(makeInstance(0, log.advanceLastIndex()));
    log.append(makeInstance(0, log.advanceLastIndex()));
    assertEquals(1, log.at(1L).getIndex());
    assertEquals(2, log.at(2L).getIndex());
  }

  @Test
  void appendWithGap() {
    long index = 42;
    log.append(makeInstance(0, index));
    assertEquals(index, log.at(index).getIndex());
    assertEquals(index + 1, log.advanceLastIndex());
  }

  @Test
  void appendFillGaps() {
    long index = 42;
    log.append(makeInstance(0, index));
    log.append(makeInstance(0, index - 10));
    assertEquals(index + 1, log.advanceLastIndex());
  }

  @Test
  void appendHighBallotOverride() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log.append(makeInstance(lo_ballot, index, CommandType.Put));
    log.append(makeInstance(hi_ballot, index, CommandType.Del));
    assertEquals(CommandType.Del, log.at(index).getCommand().getCommandType());
  }

  @Test
  void appendLowBallotNoEffect() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log.append(makeInstance(hi_ballot, index, CommandType.Put));
    log.append(makeInstance(lo_ballot, index, CommandType.Del));
    assertEquals(CommandType.Put, log.at(index).getCommand().getCommandType());
  }

  @Test
  void commit() {
    long index1 = 1, index2 = 2;
    log.append(makeInstance(0, index1));
    log.append(makeInstance(0, index2));

    assertTrue(log.at(index1).isInProgress());
    assertTrue(log.at(index2).isInProgress());
    assertFalse(log.isExecutable());

    log.commit(index2);

    assertTrue(log.at(index1).isInProgress());
    assertTrue(log.at(index2).isCommitted());
    assertFalse(log.isExecutable());

    log.commit(index1);

    assertTrue(log.at(index1).isCommitted());
    assertTrue(log.at(index2).isCommitted());
    assertTrue(log.isExecutable());
  }

  @Test
  void commitBeforeAppend() {
    long index = 1;
    ExecutorService service = Executors.newFixedThreadPool(1);
    var f = service.submit(() -> log.commit(index));
    Thread.yield();
    log.append(makeInstance(0, log.advanceLastIndex()));
    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log.at(index).isCommitted());
  }


  @Test
  void appendCommitExecute() {
    long index = 1;

    ExecutorService service = Executors.newFixedThreadPool(1);

    var f = service.submit(() -> {
      log.execute();
    });

    log.append(makeInstance(0, index));
    log.commit(index);

    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log.at(index).isExecuted());
    assertEquals(index, log.getLastExecuted());
  }

  @Test
  void appendCommitExecuteOutOfOrder() {
    ExecutorService service = Executors.newFixedThreadPool(1);
    var f = service.submit(() -> {
      log.execute();
      log.execute();
      log.execute();
    });

    long index1 = 1, index2 = 2, index3 = 3;
    log.append(makeInstance(0, index1));
    log.append(makeInstance(0, index2));
    log.append(makeInstance(0, index3));

    log.commit(index3);
    log.commit(index2);
    log.commit(index1);

    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log.at(index1).isExecuted());
    assertTrue(log.at(index2).isExecuted());
    assertTrue(log.at(index3).isExecuted());
    assertEquals(index3, log.getLastExecuted());
  }

  @Test
  void commitUntil() {
    long ballot = 0, index;

    for (index = 1; index < 10; index++) {
      log.append(makeInstance(ballot, index));
    }
    log.append(makeInstance(ballot, index));
    log.commitUntil(index - 1, ballot);

    for (long i = 1; i < index; i++) {
      assertTrue(log.at(i).isCommitted());
    }
    assertFalse(log.at(index).isCommitted());
    assertTrue(log.isExecutable());
  }

  @Test
  void commitUntilHigherBallot() {
    long ballot = 0, index;
    for (index = 1; index < 10; index++) {
      log.append(makeInstance(ballot, index));
    }
    log.commitUntil(index - 1, ballot + 1);

    for (long i = 1; i < index; i++) {
      assertFalse(log.at(i).isCommitted());
    }

    assertFalse(log.isExecutable());
  }

  @Test
  void commitUntilCase2() {
    long ballot = 5, index;
    for (index = 1; index < 10; index++) {
      log.append(makeInstance(ballot, index));
    }

    long finalIndex = index - 1;
    var thrown = assertThrows(AssertionError.class, () -> log.commitUntil(finalIndex, ballot - 1));
    assertEquals("CommitUntil case 2", thrown.getMessage());
  }

  @Test
  void commitUntilWithGap() {
    long ballot = 0, index;
    for (index = 1; index < 10; index++) {
      if (index % 3 == 0) {
        continue;
      }
      log.append(makeInstance(ballot, index));
    }
    log.commitUntil(index - 1, ballot);
    long i;
    for (i = 1; i < index; i++) {
      if (i % 3 == 0) {
        break;
      }
      assertTrue(log.at(i).isCommitted());
    }
    for (; i < index; i++) {
      if (i % 3 == 0) {
        continue;
      }
      assertFalse(log.at(i).isCommitted());
    }
    assertTrue(log.isExecutable());
  }

  @Test
  void appendCommitUntilExecute() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, KVResult>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log.execute()));
    }
    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log.append(makeInstance(ballot, index));
    }
    index--;
    log.commitUntil(index, ballot);
    try {
      for (Future<Map.Entry<Long, KVResult>> future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    for (long i = 1; i < 11; i++) {
      assertTrue(log.at(i).isExecuted());
    }
    assertFalse(log.isExecutable());
  }

  @Test
  void appendCommitUntilExecuteTrimUntil() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, KVResult>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log.execute()));
    }
    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log.append(makeInstance(ballot, index));
    }
    index--;
    log.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    log.trimUntil(index);
    for (long i = 1; i < 11; i++) {
      assertNull(log.at(i));
    }
    assertEquals(index, log.getLastExecuted());
    assertEquals(index, log.getGlobalLastExecuted());
    assertFalse(log.isExecutable());
  }

  @Test
  void appendAtTrimmedIndex() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, KVResult>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log.execute()));
    }

    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log.append(makeInstance(ballot, index));
    }
    index--;
    log.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    log.trimUntil(index);

    for (long i = 1; i < 11; i++) {
      assertNull(log.at(i));
    }
    assertEquals(index, log.getGlobalLastExecuted());
    assertEquals(index, log.getLastExecuted());
    assertFalse(log.isExecutable());

    for (long i = 1; i < 11; i++) {
      log.append(makeInstance(ballot, i));
    }
    for (long i = 1; i < 11; i++) {
      assertNull(log.at(i));
    }
  }

  @Test
  void instances() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, KVResult>>> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(executor.submit(() -> log.execute()));
    }
    ArrayList<Instance> expected = new ArrayList<>();
    long ballot = 0;
    for (int i = 0; i < 10; i++) {
      expected.add(makeInstance(ballot, log.advanceLastIndex()));
      log.append(expected.get(expected.size() - 1));
    }
    assertEquals(expected, log.instances());

    long index = 5;
    log.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    log.trimUntil(index);
    for (int i = 0; i < index; i++) {
      expected.remove(0);
    }
    assertEquals(expected, log.instances());
  }

  @Test
  void callingStopUnblocksExecutor() {
    ExecutorService executeThread = Executors.newSingleThreadExecutor();
    executeThread.submit(() -> {
      var r = log.execute();
      assertNull(r);
    });
    Thread.yield();
    log.stop();
    executeThread.shutdown();
  }

}