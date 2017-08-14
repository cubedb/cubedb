package org.cubedb.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.cubedb.core.Constants;
import org.cubedb.core.Partition;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.offheap.OffHeapPartition;
import org.cubedb.utils.TestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class OffHeapPartitionTest {
  public static final Logger log = LoggerFactory.getLogger(OffHeapPartitionTest.class);

  public OffHeapPartition createPartition() {
    return new OffHeapPartition();
  }

  public GroupedSearchResultRow createRow(String name, String value, String metric) {
    return new GroupedSearchResultRow(
        SearchResult.FAKE_GROUP_FIELD_NAME,
        SearchResult.FAKE_GROUP_FIELD_VALUE,
        name,
        value,
        metric);
  }

  @Test
  public void testInsertData() {
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genSimpleData("f1", "c", 10);
    p.insertData(data);
    assertEquals(p.getNumRecords(), 10);
  }

  @Test
  public void testInsertDataWithExpiredIndex() throws Exception {
    // Constants.KEY_MAP_TTL
    final long oldTtl = Constants.KEY_MAP_TTL;
    final long newTtl = 10L;
    final int numRecords = 10;
    Constants.KEY_MAP_TTL = newTtl;
    // TestUtils.setFinalStatic(Constants.class.getField("KEY_MAP_TTL"),
    // newTtl);
    Thread.sleep(newTtl + 1L);
    log.info("Value of KEY_MAP_TTL is now {}", Constants.KEY_MAP_TTL);
    assertEquals(newTtl, Constants.KEY_MAP_TTL);
    OffHeapPartition p = createPartition();
    p.insertData(TestUtils.genSimpleData("f1", "c", numRecords));
    assertEquals(p.getNumRecords(), numRecords);
    boolean hasOptimizationHappened = p.optimize();
    assertFalse(hasOptimizationHappened);
    Thread.sleep(newTtl + 1L);
    hasOptimizationHappened = p.optimize();
    assertTrue(hasOptimizationHappened);
    assertEquals(p.getNumRecords(), numRecords);
    p.insertData(TestUtils.genSimpleData("f1", "c", numRecords));
    assertEquals(p.getNumRecords(), numRecords);
    // TestUtils.setFinalStatic(Constants.class.getField("KEY_MAP_TTL"),
    // oldTtl);
    Constants.KEY_MAP_TTL = oldTtl;
  }

  @Test
  public void testInsertRepetableDataLarge() {
    OffHeapPartition p = createPartition();
    for (int field = 0; field < 10; field++) {
      int numRecords = 100 * 1000;
      List<DataRow> data =
          TestUtils.genSimpleRepeatableData("f" + field, "value", "c", numRecords);
      p.insertData(data);
      assertEquals(p.getNumRecords(), field + 1);
    }
  }

  @Test
  public void testInsertDataLarge() {
    OffHeapPartition p = createPartition();
    int numRecordsCount = 0;
    for (int field = 0; field < 18; field++) {
      int numRecords = 70000 / 18;
      List<DataRow> data = TestUtils.genSimpleData("f" + field, "c", numRecords);
      p.insertData(data);
      numRecordsCount += numRecords;
      assertEquals(p.getNumRecords(), numRecordsCount);
    }
  }

  @Test
  public void testInsertSameDataLarge() {
    OffHeapPartition p = createPartition();
    int numRecords = 30000;
    int numInsertions = 10;
    List<DataRow> data = TestUtils.genSimpleData("f0", "c", numRecords);
    long t0 = System.nanoTime();
    for (int i = 0; i < numInsertions; i++) p.insertData(data);
    long t1 = System.nanoTime();

    log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, numInsertions * numRecords);
    long recordsPerSecond = numInsertions * numRecords * 1000000000L / (t1 - t0);
    log.info("That is {} records/second", recordsPerSecond);
    assertEquals(numInsertions, TestUtils.checkMatch(p, "f0", "f0_value_10", "c"));
  }

  @Test
  public void testInsertSameDataParallel() throws InterruptedException {
    final OffHeapPartition p = createPartition();
    final int numRecords = 6000;
    final int numInsertions = 20;
    final List<DataRow> data = TestUtils.genSimpleData("f0", "c", numRecords);
    class Insertor implements Runnable {
      @Override
      public void run() {
        p.insertData(data);
      }
    }

    Thread[] insertors = new Thread[numInsertions];
    final long t0 = System.nanoTime();
    p.insertData(data);
    for (int i = 0; i < numInsertions; i++) {
      insertors[i] = new Thread(new Insertor());
      insertors[i].start();
    }
    for (int i = 0; i < numInsertions; i++) {
      insertors[i].join();
    }
    final long t1 = System.nanoTime();

    log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, numInsertions * numRecords);
    long recordsPerSecond = numInsertions * numRecords * 1000000000L / (t1 - t0);
    log.info("That is {} records/second", recordsPerSecond);
    assertEquals(numInsertions + 1, TestUtils.checkMatch(p, "f0", "f0_value_10", "c"));
  }

  @Test
  public void testAddingNewColumn() {
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genSimpleData("f1", "c", 1);
    p.insertData(data);
    assertEquals(1, p.getNumRecords());
    data = TestUtils.genSimpleData("f2", "c", 1);
    p.insertData(data);
    assertEquals(2, p.getNumRecords());
  }

  @Test
  public void testGet() {
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genSimpleData("f1", "c", 1);
    p.insertData(data);
    SearchResult result = p.get(new ArrayList<Filter>(), null);
    assertEquals(1, result.getResults().size());
    assertEquals(1L, result.getResults().get(createRow("f1", "f1_value_0", "c")).longValue());
  }

  @Test
  public void testGroupedGet() {
    List<DataRow> data = new ArrayList<>();
    data.add(TestUtils.genDataRow("f1", "v1", "f2", "v1"));
    data.add(TestUtils.genDataRow("f1", "v2", "f2", "v1"));
    data.add(TestUtils.genDataRow("f1", "v2", "f2", "v1"));
    OffHeapPartition p = createPartition();
    p.insertData(data);

    // log.debug("results with grouped: {}", result);
    // 2 + null values for f1, 1 + null values for f2
    SearchResult result = p.get(new ArrayList<Filter>(), null);
    assertEquals(3, result.getResults().size());

    result = p.get(new ArrayList<Filter>(), "f1");
    // non-grouped results * (2 grouping values + null)
    assertEquals(4, result.getResults().size());
  }

  @Test
  public void testGetWithFiltersTwoColumns() {
    OffHeapPartition p = createPartition();
    List<DataRow> data = new ArrayList<DataRow>(); // genSimpleData("f1",
    // "c", 100);
    data.addAll(TestUtils.genSimpleData("f3", "c", 2));
    data.addAll(TestUtils.genSimpleRepeatableData("f2", "test_field", "c", 1));

    p.insertData(data);
    assertEquals(3, p.getNumRecords());
    assertEquals(1, TestUtils.checkMatch(p, "f2", "test_field", "c"));
  }

  @Test
  public void testGetWithFilters() {
    List<DataRow> data = new ArrayList<DataRow>(); // genSimpleData("f1",
    // "c", 100);
    data.addAll(TestUtils.genSimpleData("f1", "c", 10));
    data.addAll(TestUtils.genSimpleData("f2", "c", 10));
    data.addAll(TestUtils.genSimpleData("f3", "c", 2));
    data.addAll(TestUtils.genSimpleRepeatableData("f2", "test_field", "c", 1));
    data.addAll(TestUtils.genSimpleRepeatableData("f3", "test_field", "c", 1));

    OffHeapPartition p = createPartition();
    p.insertData(data);
    assertEquals(1, TestUtils.checkMatch(p, "f3", "test_field", "c"));
    assertEquals(1, TestUtils.checkMatch(p, "f2", "test_field", "c"));
  }

  @Test
  public void testGetWithMultiColumns() {
    int numColumns = 10;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

    p.insertData(data);
    assertTrue(TestUtils.checkMatch(p, "f_1", "f_1_0", "c") > 0);
    // assertEquals(1, checkMatch(p, "f2", "test_field", "c"));
  }

  @Test
  public void testGetWithMultiColumnsNullFilter() {
    int numColumns = 9;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

    p.insertData(data);

    p.insertData(TestUtils.genSimpleRepeatableData("f_1", "null", "c", 10));
    assertEquals(10, TestUtils.checkMatch(p, "f_1", "null", "c"));
  }

  @Test
  public void testGetWithMultiColumnsMultiFilter() {
    int numColumns = 10;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

    p.insertData(data);
    // assertTrue(checkMatch(p, "f_1", "null", "c")==0);
    // assertEquals(1, checkMatch(p, "f2", "test_field", "c"));
    for (int i = 0; i < 10; i++) {
      long c =
          TestUtils.checkMatchMultiFilter(p, "c", TestUtils.getAllFieldsFilter("f", numColumns));
      assertEquals(numColumns, c);
    }
  }

  @Test
  public void testGetWithMultiColumnsNoFilter() {
    int numColumns = 10;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

    p.insertData(data);
    assertEquals(0, TestUtils.checkMatch(p, "f_1", "null", "c"));
    assertEquals(0, TestUtils.checkMatch(p, "f_1", "non_existant_test_field", "c"));
    assertEquals(
        0, TestUtils.checkMatch(p, "non_existant_column_f_1", "non_existant_test_field", "c"));
    SearchResult result = null;
    for (int i = 0; i < 10; i++) {
      result = p.get(new ArrayList<Filter>(), null);
    }
    long c = result.getResults().values().stream().mapToLong(Long::longValue).sum();
    assertTrue(0 < c);
  }

  @Test
  public void testGetWithMultiColumnsNoFilterParallel() throws InterruptedException {
    int numColumns = 11;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);
    int numSearches = Runtime.getRuntime().availableProcessors();
    p.insertData(data);
    class Searcher implements Runnable {
      @Override
      public void run() {
        p.get(new ArrayList<Filter>(), null);
      }
    }

    Thread[] searchers = new Thread[numSearches];
    p.insertData(data);
    for (int i = 0; i < numSearches; i++) {
      searchers[i] = new Thread(new Searcher());
      searchers[i].start();
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < numSearches; i++) {
      searchers[i].join();
    }
    long t1 = System.nanoTime();
    log.info("Took {} ms to search {} records", (t1 - t0) / 1000000, data.size() * numSearches);
    long recordsPerSecond = data.size() * numSearches * 1000000000L / (t1 - t0);
    log.info("That is {} records/second", recordsPerSecond);
  }

  @Test
  public void testGetWithMultiColumnsOneFilterParallel() throws InterruptedException {
    int numColumns = 11;
    int numValues = 3;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);
    int numSearches = Runtime.getRuntime().availableProcessors();
    p.insertData(data);
    List<Filter> f = TestUtils.getFilterFor("f_0", "f_0_0");
    // f.addAll(this.getFilterFor("f_1", "f_1_1"));
    class Searcher implements Runnable {
      @Override
      public void run() {
        p.get(f, null);
      }
    }

    Thread[] searchers = new Thread[numSearches];
    p.insertData(data);
    for (int i = 0; i < numSearches; i++) {
      searchers[i] = new Thread(new Searcher());
      searchers[i].start();
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < numSearches; i++) {
      searchers[i].join();
    }
    long t1 = System.nanoTime();
    log.info("Took {} ms to search {} records", (t1 - t0) / 1000000, data.size() * numSearches);
    long recordsPerSecond = data.size() * numSearches * 1000000000L / (t1 - t0);
    log.info("That is {} records/second", recordsPerSecond);
  }

  @Test
  public void testGetWithMultiColumnsMultiFilterSameField() {
    int numColumns = 6;
    int numValues = 6;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

    p.insertData(data);
    // assertTrue(checkMatch(p, "f_1", "null", "c")==0);
    // assertEquals(1, checkMatch(p, "f2", "test_field", "c"));
    long c =
        TestUtils.checkMatchMultiFilter(
            p, "c", "f_0", "f_0_0", "f_0", "f_0_1", "f_0", "f_0_2"); // ,
    // "f_0",
    // "f_0_3");
    assertTrue(c > 0);

    c =
        TestUtils.checkMatchMultiFilter(
            p, "c", "f_0", "f_0_0", "f_0", "f_0_1", "f_0", "f_0_2", "f_0", "f_0_3");
    assertTrue(c > 0);
  }

  @Test
  public void testGetSelectiveFilterSameField() {
    OffHeapPartition p = createPartition();

    // List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns,
    // numValues);

    p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1", "f3", "v1"));
    p.insert(TestUtils.genDataRow("f1", "v2", "f2", "v1", "f3", "v1"));
    p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1", "f3", "v1"));
    p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v2", "f3", "v2"));

    SearchResult result = p.get(TestUtils.getFilterFor("f1", "v2"), null);
    long firstColumnCount = 0;
    long secondColumnCount = 0;
    for (Entry<GroupedSearchResultRow, Long> e : result.getResults().entrySet()) {
      GroupedSearchResultRow row = e.getKey();
      log.info("{}={}", row, e.getValue());
      if (row.getFieldName().equals("f1")) {
        firstColumnCount += e.getValue();
      }
      if (row.getFieldName().equals("f2")) {
        secondColumnCount += e.getValue();
      }
    }
    // log.info("Found {} results", firstColumnCount);
    //assertEquals(0l, result.getResults().get(createRow("f3", "v2", "c")).longValue());
    assertEquals(1L, result.getResults().get(createRow("f3", "v1", "c")).longValue());
    assertEquals(4L, firstColumnCount);
    assertEquals(1L, secondColumnCount);
    assertEquals(
        2L,
        p.get(TestUtils.getFilterFor("f3", "v1"), null)
        .getResults()
        .get(createRow("f1", "v1", "c"))
        .longValue());
  }

  @Test
  public void insertNullTest() {
    OffHeapPartition p = createPartition();
    p.insert(TestUtils.genDataRow("not_null", "1", "null", null));
    p.get(TestUtils.getFilterFor("null", null), null);
  }

  @Test
  public void serDeTest() throws IOException {
    final Kryo kryo = new Kryo();
    File destination = File.createTempFile("partition_", ".gz");
    final Output output = new Output(new GZIPOutputStream(new FileOutputStream(destination)));
    int numColumns = 6;
    int numValues = 6;
    OffHeapPartition p = createPartition();
    List<DataRow> data = TestUtils.genMultiColumnData("f1", numColumns, numValues);
    data.addAll(TestUtils.genMultiColumnData("f2", numColumns, numValues));
    p.insertData(data);
    log.info("Writing partition with {} records to {}", data.size(), destination.getAbsolutePath());
    long t0 = System.nanoTime();
    kryo.writeObject(output, p);
    output.close();
    long t1 = System.nanoTime();
    log.info("Took {} ms to write {} records", (t1 - t0) / 1000000, data.size());

    // Now reading the file
    Input input = new Input(new GZIPInputStream(new FileInputStream(destination)));
    t0 = System.nanoTime();
    OffHeapPartition newP = kryo.readObject(input, OffHeapPartition.class);
    t1 = System.nanoTime();
    assertEquals(p.getNumRecords(), newP.getNumRecords());
    long cP =
        TestUtils.checkMatchMultiFilter(
            p, "c", "f1_0", "f1_0_0", "f1_0", "f1_0_1", "f1_0", "f1_0_2"); // ,
    long cNp =
        TestUtils.checkMatchMultiFilter(
            newP, "c", "f1_0", "f1_0_0", "f1_0", "f1_0_1", "f1_0", "f1_0_2"); // ,
    assertEquals(cP, cNp);
    log.info("Took {} ms to read {} records", (t1 - t0) / 1000000, data.size());
    log.info("{}", newP.getStats());
    destination.deleteOnExit();
  }

  @Test
  public void faultySerDeTest() throws IOException {
    List<DataRow> in = TestUtils.readFromJsonFile("src/test/resources/dumps/faulty.json.gz");
    Partition p = createPartition();
    for (DataRow r : in) {
      p.insert(r);
    }
    File dump = TestUtils.dumpToTmpFile(p);

    Input input = new Input(new GZIPInputStream(new FileInputStream(dump)));
    long t0 = System.nanoTime();
    Kryo kryo = new Kryo();
    Partition newP = (Partition) kryo.readClassAndObject(input);
    long t1 = System.nanoTime();
    log.info("Took {} ms to read {} records", (t1 - t0) / 1000000, newP.getNumRecords());
    log.info("{}", newP.getStats());
  }

  @Test
  public void counterConsistencyTest() throws IOException {
    Partition p = createPartition();
    p.insert(TestUtils.genDataRow("f1", "v1"));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<>(), null).getResults());
    p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1"));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    p.insert(TestUtils.genDataRow("f1", "v1"));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    p.insert(TestUtils.genDataRow("f1", "v1", "f3", null));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    p.insert(TestUtils.genDataRow("f1", "v1"));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    p.insert(TestUtils.genDataRow("new_field", null));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
  }

  @Test
  public void counterConsistencyTestLarge() throws IOException {
    int numColumns = 5;
    int numValues = 6;
    Partition p = createPartition();

    for (DataRow r : TestUtils.genMultiColumnData("f1", numColumns, numValues)) p.insert(r);
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    for (DataRow r : TestUtils.genMultiColumnData("f1", numColumns + 1, numValues)) p.insert(r);
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    for (DataRow r : TestUtils.genMultiColumnData("f1", numColumns + 1, numValues + 1)) p.insert(r);
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
    p.insert(TestUtils.genDataRow("new_field", null));
    TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>(), null).getResults());
  }
}
