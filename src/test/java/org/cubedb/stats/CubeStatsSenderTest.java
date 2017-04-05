package org.cubedb.stats;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


import org.cubedb.core.MultiCube;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.offheap.OffHeapPartitionTest;
import org.cubedb.stats.CubeStatsSender;
import org.cubedb.stats.StatsSender;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableSet;

public class CubeStatsSenderTest {
	public static final Logger log = LoggerFactory.getLogger(CubeStatsSenderTest.class);
	
	public static abstract class TestMultiCube implements MultiCube{

				@Override
		public Map<GroupedSearchResultRow, Long> get(String cubeName, String fromPartition, String toPartition,
				List<Filter> filters) {
			return null;
		}

		@Override
		public Map<GroupedSearchResultRow, Long> get(String cubeName, String fromPartition, String toPartition,
				List<Filter> filters, String groupBy) {
			return null;
		}

		@Override
		public Map<GroupedSearchResultRow, Long> get(String cubeName, int lastNum, List<Filter> filters) {
			return null;
		}

		@Override
		public Map<GroupedSearchResultRow, Long> get(String cubeName, int lastNum, List<Filter> filters,
				String groupBy) {
			return null;
		}

		@Override
		public boolean hasCube(String cubeName) {
			return false;
		}

		@Override
		public void save(String path) throws FileNotFoundException, IOException {
			
		}

		@Override
		public int deleteCube(String cubeName, String fromPartition, String toPartition) {
			return 0;
		}

		@Override
		public int deleteCube(String cubeName, int keepLastN) {
			return 0;
		}

		@Override
		public int deleteCube(int keepLastN) {
			return 0;
		}

		@Override
		public int deleteCube(String fromPartition, String toPartition) {
			return 0;
		}

		@Override
		public String getPath() {
			return null;
		}

		@Override
		public void load(String path) {
			
		}

		@Override
		public Map<String, Object> getStats() {
			return null;
		}

		@Override
		public void saveAsJson(String path) throws IOException {
			
		}

		@Override
		public int optimize() {
			return 0;
		}
		
	}
	
	
	public static MultiCube getTestMultiCube(final List<DataRow> resultingRows){
		MultiCube cube = new TestMultiCube() {
			
			@Override
			public void insert(List<DataRow> data) {
				assertEquals(data.size(), 2);
				resultingRows.clear();
				resultingRows.addAll(data);
			}
		};
		return cube;
	}
	
	@Test
	public void testFullStats() {
		List<DataRow> results = new ArrayList<DataRow>();
		StatsSender stats = new CubeStatsSender(getTestMultiCube(results));
		Collection<String> flags = ImmutableList.of( "f1", "p", "action");
		stats.send("action_name", "cube_name", true, true, flags);
		for(DataRow row: results){
			assertTrue(row.getCubeName().startsWith(StatsSender.CUBE_NAME));
			log.info(row.toString());
			assertEquals(row.getFields().get("cube_name"), "cube_name");
			assertEquals(row.getFields().get("f1"), StatsSender.FLAG_FIELD_VALUE);
			assertNull(row.getFields().get("p"));
			assertEquals(row.getFields().get("action"), "action_name");
		}
		
	}

}
