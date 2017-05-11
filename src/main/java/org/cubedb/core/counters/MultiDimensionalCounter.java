package org.cubedb.core.counters;

import java.util.function.ObjLongConsumer;

public interface MultiDimensionalCounter {
public void add(long values, int... fields);
//public void forEach(ObjLongConsumer<int[]> action);
public void forEach(QuartIntLongConsumer action);
}
