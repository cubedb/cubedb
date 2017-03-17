package org.cubedb.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.cubedb.utils.CubeUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CubeUtilsTest {
	public static final Logger log = LoggerFactory.getLogger(CubeUtilsTest.class);

	protected <T> void ensureSameLengthAndNoDuplicates(List<T> in, int numParts){
		List<List<T>> out = CubeUtils.partitionList(in, numParts);
		int totalSize = out.stream().mapToInt(List::size).sum();
		int minSize = out.stream().mapToInt(List::size).min().orElse(0);
		int maxSize = out.stream().mapToInt(List::size).max().orElse(0);
		assertTrue(maxSize - minSize <=1);
		//log.info("{}", out.stream().mapToInt(List::size).toArray());
		assertEquals(in.size(), totalSize);
		assertEquals(in, out.stream().flatMap(List::stream).collect(Collectors.toList()));
	}
	@Test
	public void test() {
		int max_parts = 96;
		int max_length = 480;
		for(int l =0;l < max_length;l++)
		{
			List<Integer> list = new ArrayList<Integer>(max_length);
			for(int j=0;j<l;j++)
			{
				list.add(j);
			}
		for(int p = 1; p < max_parts;p++){
			//log.info("l: {}, p: {}", l, p);
			ensureSameLengthAndNoDuplicates(list, p);
		}
		}		
	}

	
	@Test
	public void test12_8() {
		int max_parts = Runtime.getRuntime().availableProcessors();
		int max_length = 12;
			List<Integer> list = new ArrayList<Integer>(max_length);
			for(int j=0;j<max_length;j++)
			{
				list.add(j);
			}
			List<List<Integer>> out = CubeUtils.partitionList(list, max_parts);
			log.info("Out is {}", out);
			assertEquals(max_parts, out.size());
		}
	
	@Test
	public void testCutSuffix()
	{
		assertArrayEquals(new short[]{0,0,1,2,3}, CubeUtils.cutZeroSuffix(new short[]{0,0,1,2,3,0,0,0}));
		assertArrayEquals(new short[]{1,2,3}, CubeUtils.cutZeroSuffix(new short[]{1,2,3,0,0,0}));
		assertArrayEquals(new short[]{1,2,3}, CubeUtils.cutZeroSuffix(new short[]{1,2,3}));
		assertArrayEquals(new short[]{}, CubeUtils.cutZeroSuffix(new short[]{0,0,0,0,0}));
		assertArrayEquals(new short[]{}, CubeUtils.cutZeroSuffix(new short[]{}));
	
	}
}
