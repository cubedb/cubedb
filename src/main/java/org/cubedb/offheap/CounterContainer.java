package org.cubedb.offheap;

class CounterContainer {

	private final long[][][][] sideCounters;

	CounterContainer(long[][][][] sideCounters) {
		this.sideCounters = sideCounters;
	}

	void add(int fieldId, int columnValueId, int groupFieldValueId, int metricIndex, long metricValue) {
		sideCounters[fieldId][columnValueId][groupFieldValueId][metricIndex] += metricValue;
	}

	long[][][][] getCounters() {
		return sideCounters;
	}

}
