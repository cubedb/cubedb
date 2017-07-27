package org.cubedb.core;

import java.util.Map.Entry;
import java.util.stream.Stream;

public interface KeyMap {
  Integer get(byte[] b);

  void put(byte[] k, int v);

  int size();

  Stream<Entry<byte[], Integer>> entrySet();
}
