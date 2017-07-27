package org.cubedb.core.beans;

public class SearchResultRow {

  protected final String fieldName;
  protected final String fieldValue;
  protected final String metricName;

  public SearchResultRow(String fieldName, String fieldValue, String metricName) {
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.metricName = metricName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldValue() {
    return fieldValue;
  }

  public String getMetricName() {
    return metricName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
    result = prime * result + ((fieldValue == null) ? 0 : fieldValue.hashCode());
    result = prime * result + ((metricName == null) ? 0 : metricName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    SearchResultRow other = (SearchResultRow) obj;
    if (fieldName == null) {
      if (other.fieldName != null) return false;
    } else if (!fieldName.equals(other.fieldName)) return false;
    if (fieldValue == null) {
      if (other.fieldValue != null) return false;
    } else if (!fieldValue.equals(other.fieldValue)) return false;
    if (metricName == null) {
      if (other.metricName != null) return false;
    } else if (!metricName.equals(other.metricName)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "SearchResult [fieldName="
      + fieldName
      + ", fieldValue="
      + fieldValue
      + ", metricName="
      + metricName
      + "]";
  }
}
