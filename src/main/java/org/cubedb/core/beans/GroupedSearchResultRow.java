package org.cubedb.core.beans;

import java.util.Objects;

public class GroupedSearchResultRow extends SearchResultRow {

  private final String groupFieldName;
  private final String groupFieldValue;
  private final int cachedHashCode;

  public GroupedSearchResultRow(
      String groupFieldName,
      String groupFieldValue,
      String fieldName,
      String fieldValue,
      String metricName) {
    super(fieldName, fieldValue, metricName);
    this.groupFieldName = groupFieldName;
    this.groupFieldValue = groupFieldValue;
    this.cachedHashCode = this.buildHashCode();
  }

  public String getGroupFieldName() {
    return groupFieldName;
  }

  public String getGroupFieldValue() {
    return groupFieldValue;
  }

  protected int buildHashCode() {
    return Objects.hash(groupFieldName, groupFieldValue, fieldName, fieldValue, metricName);
    /*
     * final int prime = 31; int result = super.hashCode(); result = prime *
     * result + ((groupFieldName == null) ? 0 : groupFieldName.hashCode());
     * result = prime * result + ((groupFieldValue == null) ? 0 :
     * groupFieldValue.hashCode()); return result;
     */
  }

  @Override
  public int hashCode() {
    return cachedHashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (this.hashCode() != obj.hashCode()) return false;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    GroupedSearchResultRow other = (GroupedSearchResultRow) obj;

    if (groupFieldName == null) {
      if (other.groupFieldName != null) return false;
    } else if (!groupFieldName.equals(other.groupFieldName)) return false;
    if (groupFieldValue == null) {
      if (other.groupFieldValue != null) return false;
    } else if (!groupFieldValue.equals(other.groupFieldValue)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "GroupedSearchResult ["
        + "fieldName="
        + fieldName
        + ", fieldValue="
        + fieldValue
        + ", groupFieldName="
        + groupFieldName
        + ", groupFieldValue="
        + groupFieldValue
        + ", metricName="
        + metricName
        + "]";
  }
}
