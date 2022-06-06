package log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import command.Command;
import command.Command.CommandType;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  void Constructor() {
    assertEquals(log_.getLastExecuted(), 0);
    assertEquals(log_.getGlobalLastExecuted(), 0);
    assertFalse(log_.isExecutable());
    assertNull(log_.get(0L));
    assertNull(log_.get(-1L));
    assertNull(log_.get(3L));
  }

  @Test
  void Insert() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    assertFalse(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
  }

  @Test
  void InsertUpdateInProgress() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    assertFalse(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
  }

  @Test
  void InsertUpdateCommitted() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(
        Log.insert(log, MakeInstance(ballot, index, InstanceState.kCommitted, CommandType.kPut)));
    assertFalse(
        Log.insert(log, MakeInstance(ballot, index, InstanceState.kInProgress, CommandType.kPut)));

  }

  @Test
  void InsertStale() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1, ballot = 1;
    assertTrue(Log.insert(log, MakeInstance(ballot, index, CommandType.kPut)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
    // 0 = ballot - 1
    assertFalse(Log.insert(log, MakeInstance(0, index, CommandType.kDel)));
    assertEquals(CommandType.kPut, log.get(index).getCommand().getCommandType());
  }

  @Test
  void InsertCase2Committed() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kCommitted, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void InsertCase2Executed() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kExecuted, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case2", thrown.getMessage());
  }

  @Test
  void InsertCase3() {
    HashMap<Long, Instance> log = new HashMap<>();
    long index = 1;
    var inst1 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kPut);
    var inst2 = MakeInstance(0, index, InstanceState.kInProgress, CommandType.kDel);
    Log.insert(log, inst1);
    var thrown = assertThrows(AssertionError.class, () -> Log.insert(log, inst2));
    assertEquals("Insert case3", thrown.getMessage());
  }

  @Test
  void Append() {
    log_.append(MakeInstance(0));
    log_.append(MakeInstance(0));
    assertEquals(1, log_.get(1L).getIndex());
    assertEquals(2, log_.get(2L).getIndex());
  }

  @Test
  void AppendWithGap() {
    long index = 42;
    log_.append(MakeInstance(0, index));
    assertEquals(index, log_.get(index).getIndex());
    assertEquals(index + 1, log_.advanceLastIndex());
  }

  @Test
  void AppendFillGaps() {
    long index = 42;
    log_.append(MakeInstance(0, index));
    log_.append(MakeInstance(0, index - 10));
    assertEquals(index + 1, log_.advanceLastIndex());
  }

  @Test
  void AppendHighBallotOverride() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log_.append(MakeInstance(lo_ballot, index, CommandType.kPut));
    log_.append(MakeInstance(hi_ballot, index, CommandType.kDel));
    assertEquals(CommandType.kDel, log_.get(index).getCommand().getCommandType());
  }

  @Test
  void AppendLowBallotNoEffect() {
    long index = 1, lo_ballot = 0, hi_ballot = 1;
    log_.append(MakeInstance(hi_ballot, index, CommandType.kPut));
    log_.append(MakeInstance(lo_ballot, index, CommandType.kDel));
    assertEquals(CommandType.kPut, log_.get(index).getCommand().getCommandType());
  }

  @Test
  void Commit() {
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
  void CommitBeforeAppend() {
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
  void AppendCommitExecute() {
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
  void AppendCommitExecuteOutOfOrder() {
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
}