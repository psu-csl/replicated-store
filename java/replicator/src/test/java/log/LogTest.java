package log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class LogTest {

  @Test
  void log() {
    Log log = new Log();
    assertEquals(0, log.getLastExecuted());
    assertEquals(0, log.getGlobalLastExecuted());
    assertFalse(log.isExecutable());
  }
}