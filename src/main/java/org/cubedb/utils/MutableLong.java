package org.cubedb.utils;

public class MutableLong {

  long value;

  public MutableLong() {
    this(0L);
  }

  public MutableLong(long initialValue) {
    this.value = initialValue;
  }

  public MutableLong increment(long v) {
    value += v;
    return this;
  }

  public long get() {
    return value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (value ^ (value >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "MutableLong [value=" + value + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MutableLong other = (MutableLong) obj;
    if (value != other.value) return false;
    return true;
  }
}
