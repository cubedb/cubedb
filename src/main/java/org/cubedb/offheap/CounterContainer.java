package org.cubedb.offheap;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.core.beans.SearchResult;

public class CounterContainer {

	private class MultiKey {
		private final int[] keys;
		private final int hashCode;

		MultiKey(int k1, int k2, int k3, int k4) {
			keys = new int[] {k1, k2, k3, k4};
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

		public int get(int index) {
			return keys[index];
		}
	}

	private Map<MultiKey, Long> sideCounters;
    // private long[][][][] sideCounters;

	private Lookup fieldLookup;
	private Lookup metricLookup;
	private Map<String, Lookup> lookups;

	private boolean isGroupLookup;
	private String groupFieldName;

	public CounterContainer(Lookup fieldLookup, Lookup metricLookup, Map<String, Lookup> lookups) {
        this.fieldLookup = fieldLookup;
        this.metricLookup = metricLookup;
		this.lookups = lookups;
		sideCounters = new HashMap<MultiKey, Long>();
	}

	public void initSideCounters() {
		isGroupLookup = false;
		groupFieldName = SearchResult.FAKE_GROUP_FIELD_NAME;


		// sideCounters = new long[fieldLookup.size()][][][];
		// for (int i = 0; i < fieldLookup.size(); i++) {
		// 	String fieldName = fieldLookup.getKey(i);
		// 	Lookup side = lookups.get(fieldName);
		// 	sideCounters[i] = new long[side.size()][1][metricLookup.size()];
		// }
    }

	public void initGroupedSideCounters(String groupFieldName) {
		isGroupLookup = true;
		this.groupFieldName = groupFieldName;

		// sideCounters = new long[fieldLookup.size()][][][];
		// final Lookup groupSide = lookups.get(groupFieldName);
		// final int metricLookupSize = metricLookup.size();
		// final int lookupSize = fieldLookup.size();
		// for (int f = 0; f < lookupSize; f++) {
		// 	String fieldName = fieldLookup.getKey(f);
		// 	Lookup sideLookup = lookups.get(fieldName);
		// 	sideCounters[f] = new long[sideLookup.size()][groupSide.size()][metricLookupSize];
		// }
    }

    public void add(int fieldId, int columnValueId, int groupFieldValueId, int metricIndex, long metricValue) {
		MultiKey key = new MultiKey(fieldId, columnValueId, groupFieldValueId, metricIndex);
		sideCounters.compute(
			key, (oldKey, value) -> value == null ? metricValue : metricValue + value
		);
		// sideCounters
			// sideCounters[fieldId][columnValueId][groupFieldValueId][metricIndex] += metricValue;
    }

	public void saveResultTo(Map<GroupedSearchResultRow, Long> groupedResult) {
		Lookup groupFieldLookup = lookups.get(groupFieldName);
		sideCounters.forEach(
			(key, value) -> {
				int fieldId = key.get(0);
				String sideName = fieldLookup.getKey(fieldId);
				Lookup sideLookup = lookups.get(sideName);

				int columnValueId = key.get(1);
				String sideValue = sideLookup.getKey(columnValueId);

				int groupFieldValueId = key.get(2);
				String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(groupFieldValueId) : SearchResult.FAKE_GROUP_FIELD_VALUE;

				int metricIndex = key.get(3);
				String metricName = metricLookup.getKey(metricIndex);

				GroupedSearchResultRow row = new GroupedSearchResultRow(
					groupFieldName, groupFieldValue, sideName, sideValue, metricName
				);

				groupedResult.put(row, value);
			});
		// for (int sn = 0; sn < sideCounters.length; sn++) {
		// 	String sideName = fieldLookup.getKey(sn);
		// 	final Lookup sideLookup = lookups.get(sideName);
		// 	for (int sv = 0; sv < sideCounters[sn].length; sv++) {
		// 		String sideValue = sideLookup.getKey(sv);
		// 		for (int g = 0; g < sideCounters[sn][sv].length; g++) {
		// 			String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(g) : SearchResult.FAKE_GROUP_FIELD_VALUE;
		// 			for (int m = 0; m < sideCounters[sn][sv][g].length; m++) {
		// 				String metricName = metricLookup.getKey(m);
		// 				if(sideCounters[sn][sv][g][m] > 0){
		// 					GroupedSearchResultRow row = new GroupedSearchResultRow(
		// 						groupFieldName, groupFieldValue, sideName, sideValue, metricName
		// 					);
		// 					groupedResult.put(row, sideCounters[sn][sv][g][m]);
		// 				}
		// 			}
		// 		}
		// 	}
		// }
	}
}
