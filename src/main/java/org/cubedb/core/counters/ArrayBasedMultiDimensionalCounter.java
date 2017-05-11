package org.cubedb.core.counters;


public class ArrayBasedMultiDimensionalCounter implements MultiDimensionalCounter {
	private final long[][][] values;

	public ArrayBasedMultiDimensionalCounter(int firstDimSize, int secondDimSize, int... thirdDimSizes) {
		this.values = new long[firstDimSize][secondDimSize][];
		for (int i = 0; i < firstDimSize; i++)
			for (int j = 0; j < secondDimSize; j++) {
				this.values[i][j] = new long[thirdDimSizes[j]];
			}
	}

	@Override
	public void add(long value, final int... fields) {
		final int f0 = fields[0];
		final int f1 = fields[1];
		final int f2 = fields[2];
		values[f0][f1][f2]+=value;

	}

	@Override
	public void forEach(QuartIntLongConsumer action) {
		for (int i = 0; i < values.length; i++)
			for (int j = 0; j < values[i].length; j++)
				for (int k = 0; k < values[i][j].length; k++) {
					final long v = values[i][j][k];
					if(v>0)
						action.accept(i, j, k, 0, v);
				}

	}

}
