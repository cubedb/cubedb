package org.cubedb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Before;
import org.junit.Test;

/** Created by krash on 28.06.17. */
public abstract class BaseColumnTest {

  private Column column;

  @Before
  public void setUp() {
    column = createColumn();
  }

  protected abstract Column createColumn();

  @Test
  public void testInitialState() {
    assertEquals(0, column.getNumRecords());
    assertEquals(0, column.size());
  }

  @Test
  public void testAppend() {
    for (int i = 1; i <= 10; i++) {
      column.append(i);
    }
    assertEquals(10, column.getNumRecords());
    for (int i = 1; i <= 10; i++) {
      assertEquals(i, column.get(i - 1));
    }
    assertNotEquals(0, column.size());
  }
}
