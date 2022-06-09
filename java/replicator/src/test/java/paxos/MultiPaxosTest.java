package paxos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import log.Log;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiPaxosTest {

  protected Log log;
  protected MultiPaxos multiPaxos;
  protected Configuration config;

  @BeforeEach
  void setUp() {
    config = new Configuration();
    config.setId(0);
    log = new Log();
    multiPaxos = new MultiPaxos(log, config);
  }

  @Test
  void Constructor() {
    assertEquals(MultiPaxos.kMaxNumPeers, multiPaxos.leader());
    assertFalse(multiPaxos.isLeader());
    assertFalse(multiPaxos.isSomeoneElseLeader());
  }

  @Test
  void NextBallot() {
    for (long id = 0; id < MultiPaxos.kMaxNumPeers; id++) {
      Configuration config = new Configuration();
      config.setId(id);
      MultiPaxos mp = new MultiPaxos(log, config);
      long ballot = id;

      ballot += MultiPaxos.kRoundIncrement;
      assertEquals(ballot, mp.nextBallot());
      ballot += MultiPaxos.kRoundIncrement;
      assertEquals(ballot, mp.nextBallot());

      assertTrue(mp.isLeader());
      assertFalse(mp.isSomeoneElseLeader());
      assertEquals(id, mp.leader());
    }
  }
}