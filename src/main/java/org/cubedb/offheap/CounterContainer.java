package org.cubedb.offheap;

import java.util.Map;

import org.cubedb.core.lookups.Lookup;

class CounterContainer {

    private long[][][][] sideCounters;
	private Lookup fieldLookup;
	private Lookup metricLookup;
	protected Map<String, Lookup> lookups;

	CounterContainer(Lookup fieldLookup, Lookup metricLookup, Map<String, Lookup> lookups) {
        this.fieldLookup = fieldLookup;
        this.metricLookup = metricLookup;
		this.lookups = lookups;
	}

    protected void initSideCounters() {
        sideCounters = new long[fieldLookup.size()][][][];
        for (int i = 0; i < fieldLookup.size(); i++) {
            String fieldName = fieldLookup.getKey(i);
            Lookup side = lookups.get(fieldName);
            sideCounters[i] = new long[side.size()][1][metricLookup.size()];
        }
    }

    protected void initGroupedSideCounters(final String groupFieldName) {
        sideCounters = new long[fieldLookup.size()][][][];
        final Lookup groupSide = lookups.get(groupFieldName);
        final int metricLookupSize = metricLookup.size();
        final int lookupSize = fieldLookup.size();
        for (int f = 0; f < lookupSize; f++) {
            String fieldName = fieldLookup.getKey(f);
            Lookup side = lookups.get(fieldName);
            sideCounters[f] = new long[side.size()][groupSide.size()][metricLookupSize];
        }
    }

    void add(int fieldId, int columnValueId, int groupFieldValueId, int metricIndex, long metricValue) {
        sideCounters[fieldId][columnValueId][groupFieldValueId][metricIndex] += metricValue;
    }

    long[][][][] getCounters() {
        return sideCounters;
    }

}
