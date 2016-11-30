package org.cubedb.core.beans;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.Metric;

import org.cubedb.core.beans.GroupedSearchResultRow;

public class SearchResult {

	public final static String FAKE_GROUP_FIELD_NAME = "FAKE_GROUP_FIELD_NAME";
	public final static String FAKE_GROUP_VALUE = "FAKE_GROUP_VALUE";

	Map<GroupedSearchResultRow, Long> results;
	Map<String, Long> totalCounts;

	public static SearchResult buildEmpty(Set<String> metricNames) {
		Map<String, Long> totalCounts = new HashMap<String, Long>();
		for (String metricName : metricNames) {
			totalCounts.put(metricName, 0l);
		}
		SearchResult r = new SearchResult(new HashMap<GroupedSearchResultRow, Long>(),
										  totalCounts);
		return r;
	}

	public static SearchResult buildFromResultArray(long[][][][] sideCounters,
													long[] totalCounters,
													boolean isGroupLookup,
													String groupFieldName,
													Map<String, Lookup> lookups,
													Lookup fieldLookup,
													Lookup metricLookup) {
		final Map<GroupedSearchResultRow, Long> groupedResult = new HashMap<GroupedSearchResultRow, Long>();
		Lookup groupFieldLookup = lookups.get(groupFieldName);
		if (!isGroupLookup) {
			groupFieldName = FAKE_GROUP_FIELD_NAME;
		}
		for (int i = 0; i < sideCounters.length; i++) {
			String sideName = fieldLookup.getKey(i);
			final Lookup sideLookup = lookups.get(sideName);
			for (int j = 0; j < sideCounters[i].length; j++) {
				String sideValue = sideLookup.getKey(j);
				for (int g = 0; g < sideCounters[i][j].length; g++) {
					String groupFieldValue = isGroupLookup ? groupFieldLookup.getKey(g) : FAKE_GROUP_VALUE;
					for (int m = 0; m < sideCounters[i][j][g].length; m++) {
						String metricName = metricLookup.getKey(m);
						GroupedSearchResultRow r = new GroupedSearchResultRow();
						r.setGroupFieldName(groupFieldName);
						r.setGroupFieldValue(groupFieldValue);
						r.setFieldName(sideName);
						r.setFieldValue(sideValue);
						r.setMetricName(metricName);
						groupedResult.put(r, sideCounters[i][j][g][m]);
					}
				}
			}
		}

		Map<String, Long> totalCounts = new HashMap<String, Long>(totalCounters.length);
		for (int i = 0; i < totalCounters.length; i++)
			totalCounts.put(metricLookup.getKey(i), totalCounters[i]);
		// log.debug("Converted {} to {}", sideCounters, result);
		return new SearchResult(groupedResult, totalCounts);
	}

	public SearchResult(Map<GroupedSearchResultRow, Long> groupedResults, Map<String, Long> totalCounts) {
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

	public Map<String, Long> getTotalCounts() {
		return totalCounts;
	}

	public void setTotalCounts(Map<String, Long> totalCounts) {
		this.totalCounts = totalCounts;
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
