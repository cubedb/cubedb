package org.cubedb.core.counters;

import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.procedure.TIntLongProcedure;

public class ListListMultiDimensionalCounter implements MultiDimensionalCounter {
  private final TIntLongMap[][][] values;

  public ListListMultiDimensionalCounter(
      int firstDimSize, int secondDimSize, int... thirdDimSizes) {
    this.values = new TIntLongMap[firstDimSize][secondDimSize][];
    for (int i = 0; i < firstDimSize; i++)
      for (int j = 0; j < secondDimSize; j++) {
        this.values[i][j] = new TIntLongMap[thirdDimSizes[j]];
      }
  }

  @Override
  public void add(long value, int f0, int f1, int f2, int f3) {
    if (values[f0][f1][f2] == null) values[f0][f1][f2] = new TIntLongHashMap();
    values[f0][f1][f2].adjustOrPutValue(f3, value, value);
  }

  @Override
  public void forEach(QuartIntLongConsumer action) {
    for (int i = 0; i < values.length; i++)
      for (int j = 0; j < values[i].length; j++)
        for (int k = 0; k < values[i][j].length; k++) {
          final int f1 = i;
          final int f2 = j;
          final TIntLongMap v = values[i][j][k];
          if (v != null) {
            final int f3 = k;
            v.forEachEntry(
                new TIntLongProcedure() {

                  @Override
                  public boolean execute(int a, long b) {
                    if (b > 0) action.accept(f1, f2, f3, a, b);
                    return true;
                  }
                });
          }
        }
  }
}
