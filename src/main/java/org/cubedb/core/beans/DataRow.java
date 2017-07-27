package org.cubedb.core.beans;

import java.util.Map;

public class DataRow {

  private String cubeName;
  private String partition;
  private Map<String, String> fields;
  private Map<String, Long> counters;

  public String getCubeName() {
    return cubeName;
  }

  public void setCubeName(String cubeName) {
    this.cubeName = cubeName;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public Map<String, String> getFields() {
    return fields;
  }

  public void setFields(Map<String, String> fields) {
    this.fields = fields;
  }

  public Map<String, Long> getCounters() {
    return counters;
  }

  public void setCounters(Map<String, Long> counters) {
    this.counters = counters;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((counters == null) ? 0 : counters.hashCode());
    result = prime * result + ((cubeName == null) ? 0 : cubeName.hashCode());
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    result = prime * result + ((partition == null) ? 0 : partition.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DataRow other = (DataRow) obj;
    if (counters == null) {
      if (other.counters != null) return false;
    } else if (!counters.equals(other.counters)) return false;
    if (cubeName == null) {
      if (other.cubeName != null) return false;
    } else if (!cubeName.equals(other.cubeName)) return false;
    if (fields == null) {
      if (other.fields != null) return false;
    } else if (!fields.equals(other.fields)) return false;
    if (partition == null) {
      if (other.partition != null) return false;
    } else if (!partition.equals(other.partition)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "DataRow [cubeName="
      + cubeName
      + ", partition="
      + partition
      + ", fields="
      + fields
      + ", counters="
      + counters
      + "]";
  }
}
