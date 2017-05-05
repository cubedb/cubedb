package org.cubedb.core.beans;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import org.cubedb.core.lookups.Lookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import org.cubedb.core.CubeImpl;
import org.cubedb.core.Metric;

import org.cubedb.core.beans.GroupedSearchResultRow;

public class SearchResult {
	public static final Logger log = LoggerFactory.getLogger(SearchResult.class);
	public final static String FAKE_GROUP_FIELD_NAME = "DEFAULT_GROUP_FIELD_NAME";
	public final static String FAKE_GROUP_FIELD_VALUE = "DEFAULT_GROUP_FIELD_VALUE";

	Map<GroupedSearchResultRow, Long> results;
	Map<String, Map<String, Long>> totalCounts;

	public static SearchResult buildEmpty(Set<String> metricNames) {
		Map<String, Map<String, Long>> totalCounts = new HashMap<String, Map<String, Long>>();
		for (String metricName : metricNames) {
			Map<String, Long> groupFieldValueToCounter = new HashMap<String, Long>();
			totalCounts.put(metricName, groupFieldValueToCounter);
		}
		SearchResult r = new SearchResult(new HashMap<GroupedSearchResultRow, Long>(),
										  totalCounts);
		return r;
	}

	// TODO: re-implement with using a list instead of hashmap.
	public static SearchResult buildFromResultArray(long[][][][] sideCounters,
													long[][] totalCounters,
													boolean isGroupLookup,
													String groupFieldName,
													Map<String, Lookup> lookups,
													Lookup fieldLookup,
													Lookup metricLookup) {
		final Map<GroupedSearchResultRow, Long> groupedResult = new HashMap<GroupedSearchResultRow, Long>(10000);
		Lookup groupFieldLookup = lookups.get(groupFieldName);
		if (!isGroupLookup) {
			groupFieldName = FAKE_GROUP_FIELD_NAME;
		}
		for (int sn = 0; sn < sideCounters.length; sn++) {
			String sideName = fieldLookup.getKey(sn);
			final Lookup sideLookup = lookups.get(sideName);
			for (int sv = 0; sv < sideCounters[sn].length; sv++) {
				String sideValue = sideLookup.getKey(sv);
				for (int g = 0; g < sideCounters[sn][sv].length; g++) {
					String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(g) : FAKE_GROUP_FIELD_VALUE;
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
		log.debug("Result size is {}", groupedResult.size());

		Map<String, Map<String, Long>> totalCounts = new HashMap<String, Map<String, Long>>(totalCounters.length);
		for (int m = 0; m < totalCounters.length; m++) {
			Map<String, Long> groupValueToCounter = new HashMap(totalCounters[m].length);
			for (int g = 0; g < totalCounters[m].length; g++) {
				String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(g) : FAKE_GROUP_FIELD_VALUE;
				groupValueToCounter.put(groupFieldValue, totalCounters[m][g]);
			}
			totalCounts.put(metricLookup.getKey(m), groupValueToCounter);
		}
		// log.debug("Converted {} to {}", sideCounters, result);
		return new SearchResult(groupedResult, totalCounts);
	}

	public SearchResult(Map<GroupedSearchResultRow, Long> groupedResults,
						Map<String, Map<String, Long>> totalCounts) {
		super();
		this.results = groupedResults;
		this.totalCounts = totalCounts;
	}

	public Map<GroupedSearchResultRow, Long> getResults() {
		return results;
	}

	public void setResults(Map<GroupedSearchResultRow, Long> results) {
		this.results = results;
	}

	public Map<String, Map<String, Long>> getTotalCounts() {
		return totalCounts;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((results == null) ? 0 : results.hashCode());
		result = prime * result + ((totalCounts == null) ? 0 : totalCounts.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SearchResult other = (SearchResult) obj;
		if (results == null) {
			if (other.results != null)
				return false;
		} else if (!results.equals(other.results))
			return false;
		if (totalCounts == null) {
			if (other.totalCounts != null)
				return false;
		} else if (!totalCounts.equals(other.totalCounts))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SearchResult [results=" + results +
			", totalCounts=" + totalCounts + "]";
	}
}
