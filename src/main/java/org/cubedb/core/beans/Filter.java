package org.cubedb.core.beans;

import org.cubedb.core.Constants;

import java.util.Arrays;

public class Filter {

  public String field;
  public String[] values;

  // public FilterOperator filterOperator;
  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String[] getValues() {
    return values;
  }

  public void setValues(String[] values) {
    this.values = values;
  }

  public boolean isNullValueFilter() {
    String[] values = getValues();
    return values.length == 1 && (values[0].equals(Constants.NULL_VALUE) || values[0] == null);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Filter other = (Filter) obj;
    if (field == null) {
      if (other.field != null) return false;
    } else if (!field.equals(other.field)) return false;
    if (!Arrays.equals(values, other.values)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "Filter [field=" + field + ", values=" + Arrays.toString(values) + "]";
  }
}
