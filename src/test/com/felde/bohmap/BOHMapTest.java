package com.cfelde.bohmap;

import org.junit.Test;


public class BOHMapTest {

	@Test
	public void testNegativeAbs() {
		// Initialise a map with a hash function such that Math.abs would return a negative number.
		BOHMap map = new BOHMap(100000, object -> Integer.MIN_VALUE);

		Binary valueBinary = new Binary(new byte[] { 0, 0, 0, 0 });
		Binary keyBinary = new Binary(new byte[] { 0, 0, 0, 0 });
		map.put(keyBinary, valueBinary); // should not crash here

		keyBinary = new Binary(new byte[] { 1, 0, 0, 0 });
		map.put(keyBinary, valueBinary); // should not crash here
	}
}
