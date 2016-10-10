package org.cubedb.core.beans;

import java.util.Map;

public class SearchResult {

	Map<SearchResultRow, Long> results;
	Map<String, Long> totalCounts;

	public SearchResult(Map<SearchResultRow, Long> results, Map<String, Long> totalCounts) {
		super();
		this.results = results;
		this.totalCounts = totalCounts;
	}

	public Map<SearchResultRow, Long> getResults() {
		return results;
	}

	public void setResults(Map<SearchResultRow, Long> results) {
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
		return "SearchResult [results=" + results + ", totalCounts=" + totalCounts + "]";
	}

}
