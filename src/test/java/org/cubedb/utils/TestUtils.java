package org.cubedb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import org.cubedb.core.Cube;
import org.cubedb.core.Partition;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.offheap.OffHeapPartition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TestUtils {

  public static final Logger log = LoggerFactory.getLogger(TestUtils.class);

  public static List<DataRow> genSimpleData(
      String cubeName, String partitionString, String fieldName, String counterName, int count) {
    List<DataRow> out = new ArrayList<DataRow>();
    for (int i = 0; i < count; i++) {
      Map<String, String> fields = new HashMap<String, String>();
      Map<String, Long> counters = new HashMap<String, Long>();
      fields.put(fieldName, fieldName + "_value_" + i);
      counters.put(counterName, 1L);
      // fields.put(", value)
      DataRow r = new DataRow();
      r.setCubeName(cubeName);
      r.setPartition(partitionString);
      r.setFields(fields);
      r.setCounters(counters);
      out.add(r);
    }
    return out;
  }

  public static List<DataRow> genSimpleData(String fieldName, String counterName, int count) {
    return genSimpleData("sampleCube", "p", fieldName, counterName, count);
  }

  public static DataRow genDataRow(String... fieldsAndValues) {
    DataRow r = new DataRow();
    Map<String, Long> counters = new HashMap<String, Long>();
    counters.put("c", 1L);
    Map<String, String> fields = new HashMap<String, String>();
    for (int i = 0; i < fieldsAndValues.length; i += 2) {
      String fieldName = fieldsAndValues[i];
      String fieldValue = fieldsAndValues[i + 1];
      fields.put(fieldName, fieldValue);
      // fields.put(", value)

    }
    r.setFields(fields);
    r.setCounters(counters);
    return r;
  }

  public static List<DataRow> genDataRowList(String partitionName, String... fieldsAndValues) {
    List<DataRow> out = new ArrayList<DataRow>();
    DataRow r = genDataRow(fieldsAndValues);
    r.setPartition(partitionName);
    out.add(r);
    return out;
  }

  public static List<DataRow> genSimpleRepeatableData(
      String fieldName, String value, String counterName, int count) {
    List<DataRow> out = new ArrayList<DataRow>();
    for (int i = 0; i < count; i++) {
      Map<String, String> fields = new HashMap<String, String>();
      Map<String, Long> counters = new HashMap<String, Long>();
      fields.put(fieldName, value);
      counters.put(counterName, 1L);
      // fields.put(", value)
      DataRow r = new DataRow();
      r.setFields(fields);
      r.setCounters(counters);
      out.add(r);
    }
    return out;
  }

  public static void addPart(
      String cubeName,
      String partitionName,
      List<DataRow> out,
      int[] values,
      int curField,
      int numValues,
      String prefix) {

    if (curField == values.length) {
      Map<String, String> fields = new HashMap<String, String>();
      Map<String, Long> counters = new HashMap<String, Long>();
      for (int i = 0; i < values.length; i++) {
        int val = values[i];
        fields.put(prefix + "_" + i, prefix + "_" + i + "_" + val);
        counters.put("c", 1L);
      }
      DataRow r = new DataRow();
      r.setCubeName(cubeName);
      r.setPartition(partitionName);
      r.setFields(fields);
      r.setCounters(counters);
      out.add(r);
      return;
    }

    for (int i = 0; i < numValues; i++) {
      values[curField] = i;
      addPart(cubeName, partitionName, out, values, curField + 1, numValues, prefix);
    }
  }

  public static List<DataRow> genMultiColumnData(
      String cubeName, String partitionName, String fieldPrefix, int numFields, int numValues) {
    List<DataRow> out = new ArrayList<DataRow>();
    int[] values = new int[numFields];
    addPart(cubeName, partitionName, out, values, 0, numValues, fieldPrefix);
    return out;
  }

  public static List<DataRow> genMultiColumnData(String fieldPrefix, int numFields, int numValues) {
    return genMultiColumnData("cubeName", "p", fieldPrefix, numFields, numValues);
  }

  public static List<Filter> getFilterFor(String field, String... values) {
    Filter f = new Filter();
    f.setField(field);
    if (values != null) f.setValues(values);
    else f.setValues(new String[] {null});
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(f);
    log.info("Filter: {}", f);
    return filters;
  }

  public static long checkMatch(OffHeapPartition p, String field, String value, String metric) {
    log.info("Checking match for {}={}", field, value);
    List<Filter> f = getFilterFor(field, value);
    Map<GroupedSearchResultRow, Long> result = p.get(f, null).getResults();
    GroupedSearchResultRow row =
        new GroupedSearchResultRow(
            SearchResult.FAKE_GROUP_FIELD_NAME,
            SearchResult.FAKE_GROUP_FIELD_VALUE,
            field,
            value,
            metric);
    Long count = result.get(row);
    return count == null ? 0 : count.longValue();
  }

  public static long checkMatchMultiFilter(
      OffHeapPartition p, String metric, String... fieldsAndValues) {
    List<Filter> filters = new ArrayList<Filter>();
    for (int i = 0; i < fieldsAndValues.length; i += 2) {
      String field = fieldsAndValues[i];
      String value = fieldsAndValues[i + 1];
      filters.add(getFilterFor(field, value).get(0));
    }
    Map<GroupedSearchResultRow, Long> result = p.get(filters, null).getResults();
    long resultCount = 0L;
    for (int i = 0; i < fieldsAndValues.length; i += 2) {
      String field = fieldsAndValues[i];
      String value = fieldsAndValues[i + 1];
      GroupedSearchResultRow row =
          new GroupedSearchResultRow(
              SearchResult.FAKE_GROUP_FIELD_NAME,
              SearchResult.FAKE_GROUP_FIELD_VALUE,
              field,
              value,
              metric);
      resultCount += result.get(row).longValue();
    }
    return resultCount;
  }

  public static String[] getAllFieldsFilter(String prefix, int numFields) {
    String[] filterArray = new String[numFields * 2];
    for (int i = 0; i < numFields; i++) {
      filterArray[i * 2] = prefix + "_" + i;
      filterArray[i * 2 + 1] = prefix + "_" + i + "_0";
    }
    return filterArray;
  }

  public static long runInParallel(int numThreads, IntConsumer f) throws InterruptedException {
    class Runner implements Runnable {
      private int index;

      public Runner(int index) {
        this.index = index;
      }

      @Override
      public void run() {
        f.accept(this.index);
      }
    }

    Thread[] runners = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      runners[i] = new Thread(new Runner(i));
      runners[i].start();
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < numThreads; i++) {
      runners[i].join();
    }
    long t1 = System.nanoTime();
    return t1 - t0;
  }

  public static List<DataRow> readFromJsonFile(String gzipFile) throws IOException {
    GZIPInputStream source = new GZIPInputStream(new FileInputStream(new File(gzipFile)));
    String content = IOUtils.toString(source);
    return JsonIterator.deserialize(content, new TypeLiteral<List<DataRow>>() {});
  }

  public static List<DataRow> readFromJsonFileLineByLine(String gzipFile) throws IOException {
    GZIPInputStream source = new GZIPInputStream(new FileInputStream(new File(gzipFile)));
    String content = IOUtils.toString(source);
    List<DataRow> out = new ArrayList<>();
    TypeLiteral<DataRow> t = new TypeLiteral<DataRow>() {};

    for (String line : content.split("\n")) out.add(JsonIterator.deserialize(line, t));

    return out;
  }

  public static <V> void compareSets(Set<V> left, Set<V> right) {
    for (V f : left) {
      if (!right.contains(f)) {
        log.info("{} does not exist in right", f);
        assertTrue(false);
      }
    }
    for (V f : right) {
      if (!left.contains(f)) {
        log.info("{} does not exist in left", f);
        assertTrue(false);
      }
    }
  }

  public static <K, V> void testGroupings(
      Collection<K> left, Collection<K> right, Function<K, V> grouper) {
    Map<V, Set<K>> leftGroup =
        left.stream().collect(Collectors.groupingBy(grouper, Collectors.toSet()));
    Map<V, Set<K>> rightGroup =
        right.stream().collect(Collectors.groupingBy(grouper, Collectors.toSet()));
    assertEquals(leftGroup.size(), rightGroup.size());
    assertEquals(leftGroup.keySet(), rightGroup.keySet());
    assertEquals(
        leftGroup.values().stream().mapToInt(e -> e.size()).max(),
        rightGroup.values().stream().mapToInt(e -> e.size()).max());
    int inOut = 0;
    int outIn = 0;
    compareSets(leftGroup.keySet(), rightGroup.keySet());
    for (V f : leftGroup.keySet()) {
      if (!rightGroup.containsKey(f)) {
        log.info("{} does not exist in out", f);
        inOut++;
        assertTrue(false);
      }
      compareSets(leftGroup.get(f), rightGroup.get(f));
    }
    for (V f : rightGroup.keySet()) {
      if (!leftGroup.containsKey(f)) {
        log.info("{} does not exist in out", f);
        inOut++;
        assertTrue(false);
      }
      compareSets(leftGroup.get(f), rightGroup.get(f));
    }
    //assertEquals(leftGroup.values(), rightGroup.values());
    //log.info(new Genson().serialize(leftGroup));
    //log.info(new Genson().serialize(rightGroup));
    assertEquals(leftGroup, rightGroup);
  }

  public static File dumpToTmpFile(Partition p) throws IOException {
    File destination = File.createTempFile("partition_", ".gz");
    Output output = new Output(new GZIPOutputStream(new FileOutputStream(destination)));
    long t0 = System.nanoTime();
    Kryo kryo = new Kryo();
    kryo.writeClassAndObject(output, p);
    output.close();
    long t1 = System.nanoTime();
    log.info("Took {} ms to write {} records", (t1 - t0) / 1000000, p.getNumRecords());
    destination.deleteOnExit();
    return destination;
  }

  public static File dumpCubeToTmpFile(Cube c) throws IOException {
    File destination = File.createTempFile("cube_", ".snappy");
    c.save(destination.getAbsolutePath());
    destination.deleteOnExit();
    return destination;
  }

  public static File dumpCubeToTmpGzipFile(Cube c) throws IOException {
    File destination = File.createTempFile("cube_", ".gz");
    c.save(destination.getAbsolutePath());
    destination.deleteOnExit();
    return destination;
  }

  public static File dumpCubeToTmpFileAsJson(Cube c, String cubeName) throws IOException {
    final long t0 = System.nanoTime();
    File destination = File.createTempFile("cube_", ".gz");
    c.saveAsJson(destination.getAbsolutePath(), cubeName);
    log.info("Destination file is {}", destination.getAbsolutePath());
    destination.deleteOnExit();
    final long t1 = System.nanoTime();
    log.info("Took {} ms to write cube", (t1 - t0) / 1000000);
    return destination;
  }

  public static void ensureSidesAddUp(Map<GroupedSearchResultRow, Long> result) {
    Map<String, LongSummaryStatistics> sideTotals =
        result
        .entrySet()
        .stream()
        .collect(
            Collectors.groupingBy(
                e -> e.getKey().getFieldName(),
                Collectors.summarizingLong(e -> e.getValue().longValue())));
    int numDistinctValues =
        sideTotals
        .values()
        .stream()
        .map(LongSummaryStatistics::getSum)
        .distinct()
        .collect(Collectors.toList())
        .size();
    if (numDistinctValues != 1) {
      log.error("Sides do not add up");
      sideTotals.entrySet().forEach(e -> log.info("{}: {}", e.getKey(), e.getValue().getSum()));
    }
    assertEquals(1, numDistinctValues);
  }

  public static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);

    // remove final modifier from field
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(null, newValue);
  }
}
