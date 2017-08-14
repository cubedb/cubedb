package org.cubedb.core.tiny;

import static org.junit.Assert.assertEquals;

import org.cubedb.offheap.OffHeapColumn;
import org.cubedb.offheap.OffHeapMetric;
import org.junit.Test;

/** Created by krash on 28.06.17. */
public class TinyUtilsTest {
  @Test
  public void tinyMetricToOffHeap() throws Exception {
    TinyMetric source = new TinyMetric();
    for (int i = 1; i <= 20; i++) {
      source.append(i);
    }
    OffHeapMetric destination = TinyUtils.tinyMetricToOffHeap(source);
    assertEquals(source.getNumRecords(), destination.getNumRecords());
    for (int i = 0; i < source.getNumRecords(); i++) {
      assertEquals(source.get(i), destination.get(i));
    }
  }

  @Test
  public void tinyColumnToOffHeap() throws Exception {
    TinyColumn source = new TinyColumn();
    for (int i = 1; i <= 20; i++) {
      source.append(i);
    }

    OffHeapColumn destination = TinyUtils.tinyColumnToOffHeap(source);
    assertEquals(source.getNumRecords(), destination.getNumRecords());
    for (int i = 0; i < source.getNumRecords(); i++) {
      assertEquals(source.get(i), destination.get(i));
    }
  }
}
