package org.cubedb.core.counters;

import java.util.Arrays;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.procedure.TObjectLongProcedure;


public class MapMultiDimensionalCounter implements MultiDimensionalCounter{

	private class MultiKey {
		private final int[] keys;
		private final int hashCode;

		MultiKey(int[] keys) {
			this.keys = keys;
			hashCode = Arrays.hashCode(keys);
		}

		@Override
		public int hashCode() {
			return hashCode;
		}

		@Override
		public boolean equals(final Object other) {
			if (other == this) {
				return true;
			}
			if (other instanceof MultiKey) {
				final MultiKey otherMulti = (MultiKey) other;
				return Arrays.equals(keys, otherMulti.keys);
			}
			return false;
		}

		public int[] get() {
			return keys;
		}
	}

	private TObjectLongMap<MultiKey> counters;
	
	
	public MapMultiDimensionalCounter()
	{
		counters = new TObjectLongHashMap<>();
	}
	
	@Override
	public void add(long value, final int... fields) {
		MultiKey key = new MultiKey(fields);
		this.counters.adjustOrPutValue(key, value, value);
	}

	@Override
	public void forEach(QuartIntLongConsumer action) {
		this.counters.forEachEntry(new TObjectLongProcedure<MultiKey>() {

			@Override
			public boolean execute(MultiKey a, long b) {
				int[] k=a.get();
				action.accept(k[0], k[1], k[2], k[3], b);
				return true;
			}
		});
		
	}
	

}
