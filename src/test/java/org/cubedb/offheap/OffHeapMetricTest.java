package org.cubedb.offheap;

import org.cubedb.core.Metric;
import org.cubedb.core.BaseMetricTest;

/** Created by krash on 28.06.17. */
public class OffHeapMetricTest extends BaseMetricTest {
  @Override
  protected Metric createMetric() {
    return new OffHeapMetric();
  }
}
