package org.cubedb.core;

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.output.NullOutputStream;
import org.cubedb.core.Cube;
import org.cubedb.core.CubeImpl;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.utils.CubeUtils;
import org.cubedb.utils.TestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

public class CubeImplTest {
	public static final Logger log = LoggerFactory.getLogger(CubeImplTest.class);

	@Test
	public void testInsert() {
		int numFields = 7;
		int numValues = 3;
		int numPartitions = 12;
		Cube cube = new CubeImpl("ts");
		List<DataRow> out = new ArrayList<DataRow>();
		for (int i = 0; i < numPartitions; i++) {
			List<DataRow> data = TestUtils.genMultiColumnData("f", numFields, numValues);
			// List<DataRow> data = TestUtils.genSimpleData("f0", "c",
			// numRecords);
			String partition = "p_" + i;
			for (DataRow d : data) {
				d.setPartition(partition);
				out.add(d);
			}
		}
		log.info("Starting test");
		long t0 = System.nanoTime();
		cube.insert(out);
		long t1 = System.nanoTime();
		log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, out.size());
		long recordsPerSecond = out.size() * 1000000000l / (t1 - t0);
		log.info("That is {} records/second", recordsPerSecond);
		// assertEquals(numRecords*numPartitions, cube.getNumRecords());
		// cube.in
	}

	@Test
	public void testInsertRepeated() {
		int numFields = 7;
		int numValues = 3;
		int numPartitions = 120;
		Cube cube = new CubeImpl("ts");
		List<DataRow> out = new ArrayList<DataRow>();
		for (int i = 0; i < numPartitions; i++) {
			List<DataRow> data = TestUtils.genMultiColumnData("f", numFields, numValues);
			// List<DataRow> data = TestUtils.genSimpleData("f0", "c",
			// numRecords);
			String partition = "p_" + i;
			for (DataRow d : data) {
				d.setPartition(partition);
				out.add(d);
			}
		}
		long t0 = System.nanoTime();
		cube.insert(out);
		long t1 = System.nanoTime();
		log.info("Took {} ms to insert {} records initially", (t1 - t0) / 1000000, out.size());
		long recordsPerSecond = out.size() * 1000000000l / (t1 - t0);
		log.info("That is {} records/second", recordsPerSecond);

		log.info("Starting test");
		t0 = System.nanoTime();
		cube.insert(out);
		t1 = System.nanoTime();

		log.info("Took {} ms to insert {} records", (t1 - t0) / 1000000, out.size());
		recordsPerSecond = out.size() * 1000000000l / (t1 - t0);
		log.info("That is {} records/second", recordsPerSecond);
		// assertEquals(numRecords*numPartitions, cube.getNumRecords());
		// cube.in
	}

	@Test
	public void testGetDataRepeated() {
		int numFields = 8;
		int numValues = 3;
		int numPartitions = 120;
		int size = 0;
		Cube cube = new CubeImpl("ts");
		for (int i = 0; i < numPartitions; i++) {
			List<DataRow> data = TestUtils.genMultiColumnData("f", numFields, numValues);

			// List<DataRow> data = TestUtils.genSimpleData("f0", "c",
			// numRecords);
			String partition = "p_" + (1000 + i);
			for (DataRow d : data) {
				d.setPartition(partition);

			}
			cube.insert(data);
			size += data.size();
			TestUtils.ensureSidesAddUp(cube.get(partition, partition, new ArrayList<Filter>(), null));
			TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_" + (1100 + numPartitions), new ArrayList<Filter>(), null));
		}
		log.info("Starting test");
		long t0, t1, recordsPerSecond;
		TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_" + (1100 + numPartitions), new ArrayList<Filter>(), null));
		for (int i = 0; i < 10; i++) {
			t0 = System.nanoTime();
			Map<GroupedSearchResultRow, Long> result = cube.get("p_1000", "p_" + (1100 + numPartitions),
																TestUtils.getFilterFor("f_1", "f_1_1"), null);
			// Map<SearchResultRow, Long> result = cube.get("p_0", "p_" + (1100
			// + numPartitions), new ArrayList<Filter>());
			t1 = System.nanoTime();
			log.info("Received {} combinations", result.size());
			log.info("Took {} ms to searh among {} records", (t1 - t0) / 1000000, size);
			recordsPerSecond = size * 1000000000l / (t1 - t0);
			log.info("That is {} records/second", recordsPerSecond);
			assertEquals((numValues ) * numFields + numPartitions, result.size());
		}

		// cube.in
	}

	@Test
	public void testGetLastPartitions() {
		final int numPartitions = 30;
		final int lastCount = 10;
		class TestCubeImpl extends CubeImpl {
			public TestCubeImpl(String partitionColumn) {
				super(partitionColumn);
			}

			@Override
			public Map<GroupedSearchResultRow, Long> get(final String fromPartition, final String toPartition,
				List<Filter> filters, String groupBy) {
				assertEquals("p_" + (1000 + numPartitions - lastCount), fromPartition);
				assertEquals("p_" + (1000 + numPartitions - 1), toPartition);
				return null;
			}
		}
		Cube c = new TestCubeImpl("ts");
		List<DataRow> data = new ArrayList<DataRow>();
		for (int i = 0; i < numPartitions; i++) {
			String partitionName = "p_" + (1000 + i);
			log.info(partitionName);
			DataRow row = TestUtils.genDataRow("f", "f");
			row.setPartition(partitionName);
			data.add(row);
		}
		c.insert(data);
		c.get(lastCount, new ArrayList<Filter>(), null);
	}

	@Test
	public void testSerDe() throws FileNotFoundException, IOException {
		int numFields = 8;
		int numValues = 3;
		int numPartitions = 60;
		int size = 0;
		Cube cube = new CubeImpl("ts");
		List<DataRow> data = TestUtils.genMultiColumnData("f", numFields, numValues);
		for (int i = 0; i < numPartitions; i++) {
			String partition = "p_" + (1000 + i);
			for (DataRow d : data) {
				d.setPartition(partition);
			}
			cube.insert(data);
			size += data.size();
		}
		log.info("Starting test");
		long t0, t1, recordsPerSecond;
		t0 = System.nanoTime();
		File dstF = File.createTempFile("cube", ".snappy");
		String dst = dstF.getAbsolutePath();
		log.info("Saving cube to {}", dst);
		cube.save(dst);
		t1 = System.nanoTime();
		log.info("Took {} ms to searh save {} records", (t1 - t0) / 1000000, size);
		recordsPerSecond = size * 1000000000l / (t1 - t0);
		log.info("That is {} records/second", recordsPerSecond);
		// assertEquals((numValues + 1) * numFields + numPartitions,
		// result.size());
		dstF.deleteOnExit();
		// cube.in
	}

	@Test
	public void testSerDe2() throws FileNotFoundException, IOException {
		Cube cube = new CubeImpl("ts");
		List<DataRow> data = TestUtils.readFromJsonFile("src/test/resources/dumps/faulty.json.gz");
		cube.insert(data);
		log.info("Starting test");
		TestUtils.ensureSidesAddUp(cube.get("0", "z", new ArrayList<Filter>(), null));
		File out = TestUtils.dumpCubeToTmpFile(cube);
		Cube newCube = new CubeImpl("ts");
		newCube.load(out.getAbsolutePath());
	}

	@Test
	public void testSerDeJson() throws FileNotFoundException, IOException {
		Cube cube = new CubeImpl("ts");
		List<DataRow> inData = TestUtils.readFromJsonFile("src/test/resources/dumps/faulty.json.gz");
		cube.insert(inData);
		log.info("Starting test");
		File out = TestUtils.dumpCubeToTmpFileAsJson(cube, inData.get(0).getCubeName());
		List<DataRow> outData = TestUtils.readFromJsonFileLineByLine(out.getAbsolutePath());
		log.info("{}", outData.get(0));
		TestUtils.testGroupings(inData, outData, DataRow::getPartition);
		TestUtils.testGroupings(inData, outData, DataRow::getCounters);
		TestUtils.testGroupings(inData, outData, DataRow::getFields);
		TestUtils.testGroupings(inData, outData, DataRow::getCubeName);
	}

	@Test
	public void testJsonSerDe2() throws FileNotFoundException, IOException {
		int numFields = 6;
		int numValues = 4;
		int numPartitions = 3;
		Cube cube = new CubeImpl("ts");
		List<DataRow> inData = new ArrayList<DataRow>();
		for (int i = 0; i < numPartitions; i++) {
			List<DataRow> data = TestUtils.genMultiColumnData("f", numFields, numValues);
			String partition = "p_" + (1000 + i);
			for (DataRow d : data) {
				d.setPartition(partition);
			}

			inData.addAll(data);

		}
		cube.insert(inData);
		File dstF = File.createTempFile("cube", ".zip");
		log.info("Saving cube to {}", dstF.getAbsolutePath());
		cube.saveAsJson(dstF.getAbsolutePath(), "cubeName");
		List<DataRow> outData = TestUtils.readFromJsonFileLineByLine(dstF.getAbsolutePath());
		TestUtils.testGroupings(inData, outData, DataRow::getPartition);
		TestUtils.testGroupings(inData, outData, DataRow::getCounters);
		TestUtils.testGroupings(inData, outData, DataRow::getFields);
		TestUtils.testGroupings(inData, outData, DataRow::getCubeName);
		// dstF.deleteOnExit();
		// cube.in
	}

	@Test
	public void counterConsistencyTest() throws FileNotFoundException, IOException {
		CubeImpl cube = new CubeImpl("p");
		cube.insert(TestUtils.genDataRowList("p_1000", "f1", "v1", "old_field", "not_null"));
		TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_1000", new ArrayList<Filter>(), null));
		cube.insert(TestUtils.genDataRowList("p_1000", "f1", "v2", "old_field", "not_null"));
		TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_1000", new ArrayList<Filter>(), null));
		cube.insert(TestUtils.genDataRowList("p_1000", "new_field", "not_null"));
		TestUtils.ensureSidesAddUp(cube.get("p_100", "p_2000", new ArrayList<Filter>(), null));
	}

	// TODO: this is a testcase that highlights a known bug. Once fixed, this unit test will pass
	//@Test
	public void counterConsistencyTestLarge() throws FileNotFoundException, IOException {
		int numColumns = 1;
		int numValues = 2;
		int numPartitions = 3;
		CubeImpl cube = new CubeImpl("p");
		for (int i = 0; i < numPartitions; i++) {
			List<DataRow> data = TestUtils.genMultiColumnData("f", numColumns + numPartitions, numValues);

			// List<DataRow> data = TestUtils.genSimpleData("f0", "c",
			// numRecords);
			String partition = "p_" + (1000 + i);
			for (DataRow d : data) {
				d.setPartition(partition);
			}
			cube.insert(data);
			TestUtils.ensureSidesAddUp(cube.get(partition, partition, new ArrayList<Filter>(), null));
			TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_" + (1100 + numPartitions), new ArrayList<Filter>(), null));
		}

		cube.insert(TestUtils.genDataRowList("p_1000", "new_field", "not_null"));
		TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_" + (1100 + numPartitions), new ArrayList<Filter>(), null));
		cube.insert(TestUtils.genDataRowList("p_1100", "new_field", "not_null"));
		TestUtils.ensureSidesAddUp(cube.get("p_1000", "p_" + (1100 + numPartitions), new ArrayList<Filter>(), null));
	}
	
	static class CubeImplWithDummyWrite extends CubeImpl{

		public CubeImplWithDummyWrite(String partitionColumn) {
			super(partitionColumn);

		}
		
		@Override
		public void save(String saveFileName) throws IOException {
			Kryo kryo = CubeUtils.getKryoWithRegistrations();
			OutputStream zip = new BufferedOutputStream(new NullOutputStream());
			Output output = new Output(zip);
			kryo.writeClassAndObject(output, partitions);
			// zip.closeEntry();
			output.close();
			// zip.close();
		}
		
	}
	
	static class CubeImplWriteBuff extends CubeImpl{

		public CubeImplWriteBuff(String partitionColumn) {
			super(partitionColumn);
		}

		
		@Override
		public void save(String saveFileName) throws IOException {
			Kryo kryo = CubeUtils.getKryoWithRegistrations();
			OutputStream zip = new SnappyOutputStream(new FileOutputStream(saveFileName));
			Output output = new Output(zip);
			kryo.writeClassAndObject(output, partitions);
			// zip.closeEntry();
			output.close();
			// zip.close();
		}
		
		
	}
	
	@Test
	public void testWriteSpeed() throws IOException
	{
		Log.ERROR();
		// List<DataRow> data = TestUtils.genMultiColumnData("test", "p", "f", 5, 16);
		Cube cube = new CubeImpl("p");
		//cube.insert(data);
		Cube dummyCube = new CubeImplWithDummyWrite("p");
		
		cube.load("src/test/resources/dumps/event_cube_30_day.gz");
		
		dummyCube.load("src/test/resources/dumps/event_cube_30_day.gz");
		//dummyCube.insert(data);
		long ts1 = System.nanoTime();
		TestUtils.dumpCubeToTmpFile(dummyCube);
		long ts2 = System.nanoTime();
		File outGzipBuff = TestUtils.dumpCubeToTmpGzipFile(cube);
		long ts3 = System.nanoTime();
		File outSnappy = TestUtils.dumpCubeToTmpFile(cube);
		long ts4 = System.nanoTime();
		cube = new CubeImpl("p");
		long load_t1_start = System.nanoTime();
		cube.load(outGzipBuff.getAbsolutePath());
		long load_t1_end = System.nanoTime();
		cube = new CubeImpl("p");
		long load_t2_start = System.nanoTime();
		cube.load(outSnappy.getAbsolutePath());
		long load_t2_end = System.nanoTime();
		outSnappy.length();
		log.info("Writing with snappy file took {} ms", (ts4-ts3)/1000000);
		log.info("Writing with gzip compression took {} ms", (ts3-ts2)/1000000);
		log.info("Writing to fake file took {} ms", (ts2-ts1)/1000000);
		
		log.info("Reading from gzip file took {}ms", (load_t1_end-load_t1_start) / 1000000);
		log.info("Reading from snappy file took {}ms", (load_t2_end-load_t2_start) / 1000000);
	}
}
