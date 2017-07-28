package org.cubedb.core;

public interface Metric {
  void append(long value);

  long get(int index);

  void incrementBy(int index, long value);

  long size();

  void free();

  public int getNumBuffers();

  public int getNumRecords();

  boolean isTiny();
}
