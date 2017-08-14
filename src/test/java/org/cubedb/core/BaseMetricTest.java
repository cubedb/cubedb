package org.cubedb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Before;
import org.junit.Test;

/** Created by krash on 28.06.17. */
public abstract class BaseMetricTest {

  private Metric metric;

  @Before
  public void setUp() {
    metric = createMetric();
  }

  protected abstract Metric createMetric();

  @Test
  public void testInitialState() {
    assertEquals(0, metric.getNumRecords());
    assertEquals(0, metric.size());
  }

  @Test
  public void testAppend() {
    for (int i = 1; i <= 10; i++) {
      metric.append(i);
    }
    assertEquals(10, metric.getNumRecords());
    for (int i = 1; i <= 10; i++) {
      assertEquals(i, metric.get(i - 1));
    }
    assertNotEquals(0, metric.size());
  }

  @Test
  public void testIncrement() {
    metric.append(0);
    metric.incrementBy(0, 100);
    assertEquals(100, metric.get(0));
  }
}
