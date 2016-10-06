package com.badoo.cube.offheap;


import org.cubedb.offheap.CachedCountersOffHeapPartition;
import org.cubedb.offheap.OffHeapPartition;

public class CachedCountersOffHeapPartitionTest extends OffHeapPartitionTest{

	public OffHeapPartition createPartition(){
		return new CachedCountersOffHeapPartition();
	}

}
