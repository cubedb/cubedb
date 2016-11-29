package com.badoo.cube.offheap;

import static org.junit.Assert.*;

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

import org.cubedb.core.Constants;
import org.cubedb.core.Partition;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.offheap.OffHeapPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.badoo.cube.utils.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class OffHeapPartitionTest {
	public static final Logger log = LoggerFactory.getLogger(OffHeapPartitionTest.class);

	public OffHeapPartition createPartition() {
		return new OffHeapPartition();
	}

	@Test
	public void testInsertData() {
		OffHeapPartition p = createPartition();
		List<DataRow> data = TestUtils.genSimpleData("f1", "c", 10);
		p.insertData(data);
		assertEquals(p.getNumRecords(), 10);
	}

	@Test
	public void testInsertDataWithExpiredIndex() throws NoSuchFieldException, SecurityException, Exception {
		// Constants.KEY_MAP_TTL
		long oldTtl = Constants.KEY_MAP_TTL;
		long newTtl = 10l;
		int numRecords = 10;
		Constants.KEY_MAP_TTL = newTtl;
		//TestUtils.setFinalStatic(Constants.class.getField("KEY_MAP_TTL"), newTtl);
		Thread.sleep(newTtl + 1l);
		log.info("Value of KEY_MAP_TTL is now {}", Constants.KEY_MAP_TTL);
		assertEquals(newTtl, Constants.KEY_MAP_TTL);
		OffHeapPartition p = createPartition();
		p.insertData(TestUtils.genSimpleData("f1", "c", numRecords));
		assertEquals(p.getNumRecords(), numRecords);
		boolean hasOptimizationHappened = p.optimize();
		assertFalse(hasOptimizationHappened);
		Thread.sleep(newTtl + 1l);
		hasOptimizationHappened = p.optimize();
		assertTrue(hasOptimizationHappened);
		assertEquals(p.getNumRecords(), numRecords);
		p.insertData(TestUtils.genSimpleData("f1", "c", numRecords));
		assertEquals(p.getNumRecords(), numRecords);
		//TestUtils.setFinalStatic(Constants.class.getField("KEY_MAP_TTL"), oldTtl);
		Constants.KEY_MAP_TTL = oldTtl;
	}

	@Test
	public void testInsertRepetableDataLarge() {
		OffHeapPartition p = createPartition();
		for (int field = 0; field < 10; field++) {
			int num_records = 100 * 1000;
			List<DataRow> data = TestUtils.genSimpleRepeatableData("f" + field, "value", "c", num_records);
			p.insertData(data);
			assertEquals(p.getNumRecords(), field + 1);
		}
	}

	@Test
	public void testInsertDataLarge() {
		OffHeapPartition p = createPartition();
		int num_records_count = 0;
		for (int field = 0; field < 18; field++) {
			int num_records = 70000 / 18;
			List<DataRow> data = TestUtils.genSimpleData("f" + field, "c", num_records);
			p.insertData(data);
			num_records_count += num_records;
			assertEquals(p.getNumRecords(), num_records_count);
		}
	}

	@Test
	public void testInsertSameDataLarge() {
		OffHeapPartition p = createPartition();
		int num_records = 30000;
		int numInsertions = 10;
		List<DataRow> data = TestUtils.genSimpleData("f0", "c", num_records);
		long t0 = System.nanoTime();
		for (int i = 0; i < numInsertions; i++)
			p.insertData(data);
		long t1 = System.nanoTime();

		log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, numInsertions * num_records);
		long recordsPerSecond = numInsertions * num_records * 1000000000l / (t1 - t0);
		log.info("That is {} records/second", recordsPerSecond);
		assertEquals(numInsertions, TestUtils.checkMatch(p, "f0", "f0_value_10", "c"));
	}

	@Test
	public void testInsertSameDataParallel() throws InterruptedException {
		final OffHeapPartition p = createPartition();
		final int num_records = 6000;
		final int numInsertions = 20;
		final List<DataRow> data = TestUtils.genSimpleData("f0", "c", num_records);
		class Insertor implements Runnable {
			@Override
			public void run() {
				p.insertData(data);
			}
		}

		Thread[] insertors = new Thread[numInsertions];
		long t0 = System.nanoTime();
		p.insertData(data);
		for (int i = 0; i < numInsertions; i++) {
			insertors[i] = new Thread(new Insertor());
			insertors[i].start();
		}
		for (int i = 0; i < numInsertions; i++) {
			insertors[i].join();
		}
		long t1 = System.nanoTime();

		log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, numInsertions * num_records);
		long recordsPerSecond = numInsertions * num_records * 1000000000l / (t1 - t0);
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
		SearchResult result = p.get(new ArrayList<Filter>());
		assertEquals(2, result.getResults().size());
		assertEquals(1l, result.getResults().get(new SearchResultRow("f1", "f1_value_0", "c")).longValue());

	}

	@Test
	public void testGroupedGet() {
		OffHeapPartition p = createPartition();
		List<DataRow> data = new ArrayList<>();
		data.add(TestUtils.genDataRow("f1", "v1", "f2", "v1"));
		data.add(TestUtils.genDataRow("f1", "v2", "f2", "v1"));
		data.add(TestUtils.genDataRow("f1", "v2", "f2", "v1"));
		p.insertData(data);
		SearchResult result = p.get(new ArrayList<Filter>());
		log.debug("results with grouped: {}", result);
		// 2 + null values for f1, 1 + null values for f2
		assertEquals(5, result.getResults().size());
		// non-grouped results * (2 grouping values + null)
		assertEquals(15, result.getGroupedResults().size());
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
		OffHeapPartition p = createPartition();
		List<DataRow> data = new ArrayList<DataRow>(); // genSimpleData("f1",
														// "c", 100);
		data.addAll(TestUtils.genSimpleData("f1", "c", 10));
		data.addAll(TestUtils.genSimpleData("f2", "c", 10));
		data.addAll(TestUtils.genSimpleData("f3", "c", 2));
		data.addAll(TestUtils.genSimpleRepeatableData("f2", "test_field", "c", 1));
		data.addAll(TestUtils.genSimpleRepeatableData("f3", "test_field", "c", 1));

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
			long c = TestUtils.checkMatchMultiFilter(p, "c", TestUtils.getAllFieldsFilter("f", numColumns));
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
		assertEquals(0, TestUtils.checkMatch(p, "non_existant_column_f_1", "non_existant_test_field", "c"));
		SearchResult result = null;
		for (int i = 0; i < 10; i++) {
			result = p.get(new ArrayList<Filter>());
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
				p.get(new ArrayList<Filter>());
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
		long recordsPerSecond = data.size() * numSearches * 1000000000l / (t1 - t0);
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
				p.get(f);
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
		long recordsPerSecond = data.size() * numSearches * 1000000000l / (t1 - t0);
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
		long c = TestUtils.checkMatchMultiFilter(p, "c", "f_0", "f_0_0", "f_0", "f_0_1", "f_0", "f_0_2");// ,
		// "f_0",
		// "f_0_3");
		assertTrue(c > 0);

		c = TestUtils.checkMatchMultiFilter(p, "c", "f_0", "f_0_0", "f_0", "f_0_1", "f_0", "f_0_2", "f_0", "f_0_3");
		assertTrue(c > 0);
	}

	@Test
	public void testGetSelectiveFilterSameField() {
		OffHeapPartition p = createPartition();

		//List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns, numValues);

		p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1", "f3", "v1"));
		p.insert(TestUtils.genDataRow("f1", "v2", "f2", "v1", "f3", "v1"));
		p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1", "f3", "v1"));
		p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v2", "f3", "v2"));


		SearchResult result = p.get(TestUtils.getFilterFor("f1", "v2"));
		long firstColumnCount = 0;
		long secondColumnCount = 0;
		for (Entry<SearchResultRow, Long> e : result.getResults().entrySet()) {
			SearchResultRow row = e.getKey();
			log.info("{}={}", row, e.getValue());
			if (row.getFieldName().equals("f1")) {
				firstColumnCount += e.getValue();
			}
			if (row.getFieldName().equals("f2")) {
				secondColumnCount += e.getValue();
			}
		}
		//log.info("Found {} results", firstColumnCount);
		assertEquals(0l, result.getResults().get(new SearchResultRow("f3", "v2", "c")).longValue());
		assertEquals(1l, result.getResults().get(new SearchResultRow("f3", "v1", "c")).longValue());
		assertEquals(4l, firstColumnCount);
		assertEquals(1l, secondColumnCount);

		assertEquals(2l, p.get(TestUtils.getFilterFor("f3", "v1"))
				.getResults()
				.get(new SearchResultRow("f1", "v1", "c")).longValue());
	}

	@Test
	public void insertNullTest(){
		OffHeapPartition p = createPartition();
		p.insert(TestUtils.genDataRow("not_null", "1", "null", null));
		p.get(TestUtils.getFilterFor("null", null));
	}

	@Test
	public void serDeTest() throws FileNotFoundException, IOException
	{
		Kryo kryo = new Kryo();
		File destination = File.createTempFile("partition_", ".gz");
	    Output output = new Output(new GZIPOutputStream(new FileOutputStream(destination)));
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
		Input input = new Input(new GZIPInputStream( new FileInputStream(destination)));
		t0 = System.nanoTime();
		OffHeapPartition newP = kryo.readObject(input, OffHeapPartition.class);
		t1 = System.nanoTime();
		assertEquals(p.getNumRecords(), newP.getNumRecords());
		long c_p = TestUtils.checkMatchMultiFilter(p, "c", "f1_0", "f1_0_0", "f1_0", "f1_0_1", "f1_0", "f1_0_2");// ,
		long c_np = TestUtils.checkMatchMultiFilter(newP, "c", "f1_0", "f1_0_0", "f1_0", "f1_0_1", "f1_0", "f1_0_2");// ,
		assertEquals(c_p, c_np);
		log.info("Took {} ms to read {} records", (t1 - t0) / 1000000, data.size());
		log.info("{}", newP.getStats());
		destination.deleteOnExit();
	}

	@Test
	public void faultySerDeTest() throws FileNotFoundException, IOException
	{
		List<DataRow> in = TestUtils.readFromJsonFile("src/test/resources/dumps/faulty.json.gz");
		Partition p = createPartition();
		for(DataRow r: in)
		{
			p.insert(r);
		}
		File dump = TestUtils.dumpToTmpFile(p);

		Input input = new Input(new GZIPInputStream( new FileInputStream(dump)));
		long t0 = System.nanoTime();
		Kryo kryo = new Kryo();
		Partition newP = (Partition)kryo.readClassAndObject(input);
		long t1 = System.nanoTime();
		log.info("Took {} ms to read {} records", (t1 - t0) / 1000000, newP.getNumRecords());
		log.info("{}", newP.getStats());
	}

	@Test
	public void counterConsistencyTest() throws FileNotFoundException, IOException
	{
		Partition p = createPartition();
		p.insert(TestUtils.genDataRow("f1", "v1"));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("f1", "v1", "f2", "v1"));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("f1", "v1"));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("f1", "v1", "f3", null));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("f1", "v1"));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("new_field", null));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
	}

	@Test
	public void counterConsistencyTestLarge() throws FileNotFoundException, IOException
	{
		int numColumns = 5;
		int numValues = 6;
		Partition p = createPartition();

		for(DataRow r: TestUtils.genMultiColumnData("f1", numColumns, numValues))
			p.insert(r);
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		for(DataRow r: TestUtils.genMultiColumnData("f1", numColumns+1, numValues))
			p.insert(r);
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		for(DataRow r: TestUtils.genMultiColumnData("f1", numColumns+1, numValues+1))
			p.insert(r);
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
		p.insert(TestUtils.genDataRow("new_field", null));
		TestUtils.ensureSidesAddUp(p.get(new ArrayList<Filter>()).getResults());
	}
}
