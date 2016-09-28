package com.badoo.cube.offheap;

import static org.junit.Assert.*;

import org.cubedb.offheap.CachedCountersOffHeapPartition;
import org.cubedb.offheap.OffHeapPartition;
import org.junit.Test;

public class CachedCountersOffHeapPartitionTest extends OffHeapPartitionTest{

	public OffHeapPartition createPartition(){
		return new CachedCountersOffHeapPartition();
	}

}
