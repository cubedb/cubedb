package com.badoo.cube.offheap;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.cubedb.core.Partition;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.offheap.CachedCountersOffHeapPartition;
import org.cubedb.offheap.OffHeapPartition;
import org.junit.Test;

import com.badoo.cube.utils.TestUtils;

public class CachedCountersOffHeapPartitionTest extends OffHeapPartitionTest{

	public OffHeapPartition createPartition(){
		return new CachedCountersOffHeapPartition();
	}

	@Test
	public void testNullValuesAfterInit(){
		Partition p = createPartition();
		SearchResultRow r = new SearchResultRow("null_field", "null", "c");
		SearchResultRow notNullR = new SearchResultRow("not_null", "null", "c");
		p.insert(TestUtils.genDataRow("not_null", "not_null", "null_field", null));
		SearchResult first = p.get(new ArrayList<Filter>());
		p.insert(TestUtils.genDataRow("not_null", null, "null_field", null));
		SearchResult second = p.get(new ArrayList<Filter>());
		assertEquals(1l, first.getResults().get(r).longValue());
		assertEquals(2l, second.getResults().get(r).longValue());
		assertEquals(1l, second.getResults().get(notNullR).longValue());
		
	}
}
