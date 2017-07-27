package org.cubedb.core.tiny;

import org.cubedb.offheap.OffHeapColumn;
import org.cubedb.offheap.OffHeapMetric;

public class TinyUtils {
  public static OffHeapMetric tinyMetricToOffHeap(TinyMetric in) {
    OffHeapMetric m = new OffHeapMetric(in.offset);
    for (int i = 0; i < in.getNumRecords(); i++) {
      m.append(in.data.get(i));
    }
    return m;
  }

  public static OffHeapColumn tinyColumnToOffHeap(TinyColumn in) {
    OffHeapColumn m = new OffHeapColumn(in.offset);
    for (int i = 0; i < in.getNumRecords(); i++) {
      m.append(in.data.get(i));
    }
    return m;
  }
}
