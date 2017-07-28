package org.cubedb.utils;

import org.cubedb.core.beans.GroupedSearchResultRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.MultivaluedMap;

public class CubeUtils {
  public static final Logger log = LoggerFactory.getLogger(CubeUtils.class);

  /**
   * Split array into n sublists fairly. Difference from Lists.partition is that: 1. We don't
   * specify the size of each sublist. We specify the number of sublists. 2. Elements are
   * distributed maximally equally.
   */
  public static <T> List<List<T>> partitionList(final List<T> in, int numParts) {
    double idealSize = (double) in.size() / numParts;
    List<List<T>> out = new ArrayList<List<T>>(numParts);
    for (int i = 0; i < numParts; i++) {
      int from = (int) Math.ceil(i * idealSize);
      int to = (int) Math.min(Math.ceil((i + 1) * idealSize), in.size());
      if (from < to) out.add(in.subList(from, to));
    }
    return out;
  }

  public static <T> List<List<T>> partitionList(final List<T> in) {
    final int numProcs = Runtime.getRuntime().availableProcessors();
    final List<List<T>> out = partitionList(in, numProcs);
    // log.debug("Having {} available processors, split list into {} parts",
    // numProcs, out.size());

    return out;
  }

  public static short[] cutZeroSuffix(final short[] in) {
    for (int offset = in.length - 1; offset >= 0; offset--) {
      if (in[offset] > 0) {
        return Arrays.copyOf(in, offset + 1);
      }
    }
    return new short[0];
  }

  public static Map<String, Map<String, Map<String, Long>>> searchResultsToMap(
      Map<GroupedSearchResultRow, Long> in) {
    return in.entrySet()
        .stream()
        .collect(
            Collectors.groupingBy(
                e -> e.getKey().getFieldName(),
                Collectors.groupingBy(
                    e -> e.getKey().getFieldValue(),
                    Collectors.groupingBy(
                        e -> e.getKey().getMetricName(),
                        Collectors.summingLong(e -> e.getValue().longValue())))));
  }

  public static Map<String, Map<String, Map<String, Map<String, Long>>>> searchResultsToGroupedMap(
      Map<GroupedSearchResultRow, Long> in) {
    return in.entrySet()
        .stream()
        .collect(
            Collectors.groupingBy(
                e -> e.getKey().getFieldName(),
                Collectors.groupingBy(
                    e -> e.getKey().getFieldValue(),
                    Collectors.groupingBy(
                        e -> e.getKey().getGroupFieldValue(),
                        Collectors.groupingBy(
                            e -> e.getKey().getMetricName(),
                            Collectors.summingLong(e -> e.getValue().longValue()))))));
  }

  public static Map<String, String[]> multiValuedMapToMap(MultivaluedMap<String, String> in) {
    return in.entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toArray(new String[0])));
  }

  public static Kryo getKryoWithRegistrations() {
    // int ref = 0;
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(false);
    /*
     * kryo.register(MultiCubeImpl.class, ref++);
     * kryo.register(HashMapLookup.class, ref++);
     * kryo.register(OffHeapPartition.class, ref++);
     * kryo.register(OffHeapColumn.class, ref++);
     * kryo.register(OffHeapMetric.class, ref++);
     * kryo.register(MultiBuffer.class, ref++);
     * kryo.register(TinyColumn.class, ref++);
     * kryo.register(TinyMetric.class, ref++);
     */
    return kryo;
  }
}
