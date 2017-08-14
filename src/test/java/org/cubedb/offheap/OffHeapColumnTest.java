package org.cubedb.offheap;

import org.cubedb.core.BaseColumnTest;
import org.cubedb.core.Column;

/** Created by krash on 28.06.17. */
public class OffHeapColumnTest extends BaseColumnTest {
  @Override
  protected Column createColumn() {
    return new OffHeapColumn();
  }
}
