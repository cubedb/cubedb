package org.cubedb.core.counters;

public interface MultiDimensionalCounter {
  public void forEach(QuartIntLongConsumer action);

  void add(long value, int f0, int f1, int f2, int f3);
}
