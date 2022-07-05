package log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import command.Command;
import command.Command.CommandType;
import command.Result;
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
  private Log log_;

  public Instance MakeInstance(long ballot) {
    return new Instance(ballot, log_.advanceLastIndex(), 0, InstanceState.kInProgress,
        new Command());
  }

  public Instance MakeInstance(long ballot, long index) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command());
  }

  public Instance MakeInstance(long ballot, InstanceState state) {
    return new Instance(ballot, log_.advanceLastIndex(), 0, state, new Command());
  }

  public Instance MakeInstance(long ballot, long index, CommandType type) {
    return new Instance(ballot, index, 0, InstanceState.kInProgress, new Command(type, "", ""));
  }

  public Instance MakeInstance(long ballot, long index, InstanceState state, CommandType type) {
    return new Instance(ballot, index, 0, state, new Command(type, "", ""));
  }


  @BeforeEach
  void SetUp() {
    log_ = new Log();
    store_ = new MemKVStore();
  }

  @Test
  void constructor() {
    assertEquals(log_.getLastExecuted(), 0);
    assertEquals(log_.getGlobalLastExecuted(), 0);
    assertFalse(log_.isExecutable());
    assertNull(log_.get(0L));
    assertNull(log_.get(-1L));
    assertNull(log_.get(3L));
  }

  @Test
  void insert() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    assertFalse(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
  }

  @Test
  void insertUpdateInProgress() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    assertFalse(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
  }

  @Test
  void insertUpdateCommitted() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(
        Log.insert(log, MakeInstance(ballot, index, InstanceState.kCommitted, CommandType.kPut)));
    assertFalse(
        Log.insert(log, MakeInstance(ballot, index, InstanceState.kInProgress, CommandType.kPut)));

  }

  @Test
  void insertStale() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    // 0 = ballot - 1
    assertFalse(Log.insert(log, MakeInstance(0, index, CommandType.kDel)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
  }

  @Test
  void insertCase2Committed() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kCommitted, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void insertCase2Executed() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kExecuted, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void insertCase3() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case3", thrown.getMessage());
  }

  @Test
  void append() {
    log_.append(MakeInstance(0));
    log_.append(MakeInstance(0));
    assertEquals(1, log_.get(1L).getIndex());
    assertEquals(2, log_.get(2L).getIndex());
  }

  @Test
  void appendWithGap() {
    long index = 42;
    log_.append(MakeInstance(0, index));
    assertEquals(index, log_.get(index).getIndex());
    assertEquals(index + 1, log_.advanceLastIndex());
  }

  @Test
  void appendFillGaps() {
    long index = 42;
    log_.append(MakeInstance(0, index));
    log_.append(MakeInstance(0, index - 10));
    assertEquals(index + 1, log_.advanceLastIndex());
  }

  @Test
  void appendHighBallotOverride() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log_.append(MakeInstance(lo_ballot, index, CommandType.kPut));
    log_.append(MakeInstance(hi_ballot, index, CommandType.kDel));
    assertEquals(CommandType.kDel, log_.get(index).getCommand().getCommandType());
  }

  @Test
  void appendLowBallotNoEffect() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log_.append(MakeInstance(hi_ballot, index, CommandType.kPut));
    log_.append(MakeInstance(lo_ballot, index, CommandType.kDel));
    assertEquals(CommandType.kPut, log_.get(index).getCommand().getCommandType());
  }

  @Test
  void commit() {
    long index1 = 1, index2 = 2;
    log_.append(MakeInstance(0, index1));
    log_.append(MakeInstance(0, index2));

    assertTrue(log_.get(index1).isInProgress());
    assertTrue(log_.get(index2).isInProgress());
    assertFalse(log_.isExecutable());

    log_.commit(index2);

    assertTrue(log_.get(index1).isInProgress());
    assertTrue(log_.get(index2).isCommited());
    assertFalse(log_.isExecutable());

    log_.commit(index1);

    assertTrue(log_.get(index1).isCommited());
    assertTrue(log_.get(index2).isCommited());
    assertTrue(log_.isExecutable());
  }

  @Test
  void commitBeforeAppend() {
    long index = 1;
    ExecutorService service = Executors.newFixedThreadPool(1);
    var f = service.submit(() -> log_.commit(index));
    Thread.yield();
    log_.append(MakeInstance(0));
    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log_.get(index).isCommited());
  }


  @Test
  void appendCommitExecute() {
    long index = 1;

    ExecutorService service = Executors.newFixedThreadPool(1);

    var f = service.submit(() -> {
      log_.execute(store_);
    });

    log_.append(MakeInstance(0, index));
    log_.commit(index);

    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log_.get(index).isExecuted());
    assertEquals(index, log_.getLastExecuted());
  }

  @Test
  void appendCommitExecuteOutOfOrder() {
    ExecutorService service = Executors.newFixedThreadPool(1);
    var f = service.submit(() -> {
      log_.execute(store_);
      log_.execute(store_);
      log_.execute(store_);
    });

    long index1 = 1, index2 = 2, index3 = 3;
    log_.append(MakeInstance(0, index1));
    log_.append(MakeInstance(0, index2));
    log_.append(MakeInstance(0, index3));

    log_.commit(index3);
    log_.commit(index2);
    log_.commit(index1);

    try {
      assertNull(f.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertTrue(log_.get(index1).isExecuted());
    assertTrue(log_.get(index2).isExecuted());
    assertTrue(log_.get(index3).isExecuted());
    assertEquals(index3, log_.getLastExecuted());
  }

  @Test
  void commitUntil() {
    long ballot = 0, index;

    for (index = 1; index < 10; index++) {
      log_.append(MakeInstance(ballot, index));
    }
    log_.append(MakeInstance(ballot, index));
    log_.commitUntil(index - 1, ballot);

    for (long i = 1; i < index; i++) {
      assertTrue(log_.get(i).isCommited());
    }
    assertFalse(log_.get(index).isCommited());
    assertTrue(log_.isExecutable());
  }

  @Test
  void commitUntilHigherBallot() {
    long ballot = 0, index;
    for (index = 1; index < 10; index++) {
      log_.append(MakeInstance(ballot, index));
    }
    log_.commitUntil(index - 1, ballot + 1);

    for (long i = 1; i < index; i++) {
      assertFalse(log_.get(i).isCommited());
    }

    assertFalse(log_.isExecutable());
  }

  @Test
  void commitUntilCase2() {
    long ballot = 5, index;
    for (index = 1; index < 10; index++) {
      log_.append(MakeInstance(ballot, index));
    }

    long finalIndex = index - 1;
    var thrown = assertThrows(AssertionError.class, () -> log_.commitUntil(finalIndex, ballot - 1));
    assertEquals("CommitUntil case 2", thrown.getMessage());
  }

  @Test
  void commitUntilWithGap() {
    long ballot = 0, index;
    for (index = 1; index < 10; index++) {
      if (index % 3 == 0) { // 3, 6, 9 are gaps
        continue;
      }
      log_.append(MakeInstance(ballot, index));
    }
    // will only commitUntil 3(exclusively)
    log_.commitUntil(index - 1, ballot);
    long i;
    for (i = 1; i < index; i++) {
      if (i % 3 == 0) {
        break;
      }
      assertTrue(log_.get(i).isCommited());
    }
    for (; i < index; i++) {
      if (i % 3 == 0) {
        continue;
      }
      assertFalse(log_.get(i).isCommited());
    }
    assertTrue(log_.isExecutable());
  }

  @Test
  void appendCommitUntilExecute() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, Result>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log_.execute(store_)));
    }
    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log_.append(MakeInstance(ballot, index));
    }
    index--;
    log_.commitUntil(index, ballot);
    try {
      for (Future<Map.Entry<Long, Result>> future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    for (long i = 1; i < 11; i++) {
      assertTrue(log_.get(i).isExecuted());
    }
    assertFalse(log_.isExecutable());
  }

  @Test
  void appendCommitUntilExecuteTrimUntil() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, Result>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log_.execute(store_)));
    }
    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log_.append(MakeInstance(ballot, index));
    }
    index--;
    log_.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    log_.trimUntil(index);
    for (long i = 1; i < 11; i++) {
      assertNull(log_.get(i));
    }
    assertEquals(index, log_.getLastExecuted());
    assertEquals(index, log_.getGlobalLastExecuted());
    assertFalse(log_.isExecutable());
  }

  @Test
  void appendAtTrimmedIndex() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, Result>>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> log_.execute(store_)));
    }

    long ballot = 0, index;
    for (index = 1; index < 11; index++) {
      log_.append(MakeInstance(ballot, index));
    }
    index--;
    log_.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    log_.trimUntil(index);

    for (long i = 1; i < 11; i++) {
      assertNull(log_.get(i));
    }
    assertEquals(index, log_.getGlobalLastExecuted());
    assertEquals(index, log_.getLastExecuted());
    assertFalse(log_.isExecutable());

    for (long i = 1; i < 11; i++) {
      log_.append(MakeInstance(ballot, i));
    }
    for (long i = 1; i < 11; i++) {
      assertNull(log_.get(i));
    }
  }

  @Test
  void instancesForPrepare() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    List<Future<Map.Entry<Long, Result>>> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(executor.submit(() -> log_.execute(store_)));
    }
    ArrayList<Instance> expected = new ArrayList<>();
    long ballot = 0;
    for (int i = 0; i < 10; i++) {
      expected.add(MakeInstance(ballot));
      log_.append(expected.get(expected.size() - 1));
    }
    assertEquals(expected, log_.instancesForPrepare());

    long index = 5;
    log_.commitUntil(index, ballot);
    try {
      for (var future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    log_.trimUntil(index);
    for (int i = 0; i < index; i++) {
      expected.remove(0);
    }
    assertEquals(expected, log_.instancesForPrepare());

  }

}