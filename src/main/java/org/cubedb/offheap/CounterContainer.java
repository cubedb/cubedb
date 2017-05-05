package org.cubedb.offheap;

import java.util.Map;

import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.core.beans.SearchResult;

public class CounterContainer {

    private long[][][][] sideCounters;
	private Lookup fieldLookup;
	private Lookup metricLookup;
	private Map<String, Lookup> lookups;

	private boolean isGroupLookup;
	private String groupFieldName;

	public CounterContainer(Lookup fieldLookup, Lookup metricLookup, Map<String, Lookup> lookups) {
        this.fieldLookup = fieldLookup;
        this.metricLookup = metricLookup;
		this.lookups = lookups;
	}

	public void initSideCounters() {
		isGroupLookup = false;
		groupFieldName = SearchResult.FAKE_GROUP_FIELD_NAME;

		sideCounters = new long[fieldLookup.size()][][][];
		for (int i = 0; i < fieldLookup.size(); i++) {
			String fieldName = fieldLookup.getKey(i);
			Lookup side = lookups.get(fieldName);
			sideCounters[i] = new long[side.size()][1][metricLookup.size()];
		}
    }

	public void initGroupedSideCounters(String groupFieldName) {
		isGroupLookup = true;
		this.groupFieldName = groupFieldName;

		sideCounters = new long[fieldLookup.size()][][][];
		final Lookup groupSide = lookups.get(groupFieldName);
		final int metricLookupSize = metricLookup.size();
		final int lookupSize = fieldLookup.size();
		for (int f = 0; f < lookupSize; f++) {
			String fieldName = fieldLookup.getKey(f);
			Lookup sideLookup = lookups.get(fieldName);
			sideCounters[f] = new long[sideLookup.size()][groupSide.size()][metricLookupSize];
		}
    }

    public void add(int fieldId, int columnValueId, int groupFieldValueId, int metricIndex, long metricValue) {
        sideCounters[fieldId][columnValueId][groupFieldValueId][metricIndex] += metricValue;
    }

	public void saveResultTo(Map<GroupedSearchResultRow, Long> groupedResult) {
		Lookup groupFieldLookup = lookups.get(groupFieldName);
		for (int sn = 0; sn < sideCounters.length; sn++) {
			String sideName = fieldLookup.getKey(sn);
			final Lookup sideLookup = lookups.get(sideName);
			for (int sv = 0; sv < sideCounters[sn].length; sv++) {
				String sideValue = sideLookup.getKey(sv);
				for (int g = 0; g < sideCounters[sn][sv].length; g++) {
					String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(g) : SearchResult.FAKE_GROUP_FIELD_VALUE;
					for (int m = 0; m < sideCounters[sn][sv][g].length; m++) {
						String metricName = metricLookup.getKey(m);
						if(sideCounters[sn][sv][g][m] > 0){
							GroupedSearchResultRow row = new GroupedSearchResultRow(
								groupFieldName, groupFieldValue, sideName, sideValue, metricName
							);
							groupedResult.put(row, sideCounters[sn][sv][g][m]);
						}
					}
				}
			}
		}
	}
}
