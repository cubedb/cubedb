package org.cubedb.core;

public interface Column {
  void append(int value);

  int get(int index);

  long size();

  void free();

  public int getNumBuffers();

  public int getNumRecords();

  public boolean isTiny();
}
