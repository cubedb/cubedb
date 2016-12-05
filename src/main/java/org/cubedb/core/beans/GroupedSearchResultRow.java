package org.cubedb.core.beans;

public class GroupedSearchResultRow extends SearchResultRow {

	private String groupFieldName;
	private String groupFieldValue;

	public GroupedSearchResultRow() {
		super();
	}

	public GroupedSearchResultRow(String fieldName, String fieldValue, String metricName) {
		super(fieldName, fieldValue, metricName);
		this.groupFieldName = SearchResult.FAKE_GROUP_FIELD_NAME;
		this.groupFieldValue = SearchResult.FAKE_GROUP_VALUE;
	}

	public GroupedSearchResultRow(String groupFieldName, String groupFieldValue, String fieldName, String fieldValue, String metricName) {
		super(fieldName, fieldValue, metricName);
		this.groupFieldName = groupFieldName;
		this.groupFieldValue = groupFieldValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((groupFieldName == null) ? 0 : groupFieldName.hashCode());
		result = prime * result + ((groupFieldValue == null) ? 0 : groupFieldValue.hashCode());
		return result;
	}

	public String getGroupFieldName() {
		return groupFieldName;
	}

	public void setGroupFieldName(String groupFieldName) {
		this.groupFieldName = groupFieldName;
	}

	public String getGroupFieldValue() {
		return groupFieldValue;
	}

	public void setGroupFieldValue(String groupFieldValue) {
		this.groupFieldValue = groupFieldValue;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		GroupedSearchResultRow other = (GroupedSearchResultRow) obj;
		if (groupFieldName == null) {
			if (other.groupFieldName != null)
				return false;
		} else if (!groupFieldName.equals(other.groupFieldName))
			return false;
		if (groupFieldValue == null) {
			if (other.groupFieldValue != null)
				return false;
		} else if (!groupFieldValue.equals(other.groupFieldValue))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "GroupedSearchResult [" +
			"fieldName=" + fieldName +
			", fieldValue=" + fieldValue +
			", groupFieldName=" + groupFieldName +
			", groupFieldValue=" + groupFieldValue +
			", metricName=" + metricName
			+ "]";
	}

}
