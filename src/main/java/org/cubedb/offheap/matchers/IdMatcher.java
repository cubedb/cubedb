package org.cubedb.offheap.matchers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;
import java.util.function.IntPredicate;

public class IdMatcher {

  private final IntPredicate matcher;
  public static final Logger log = LoggerFactory.getLogger(IdMatcher.class);
  final int[] values;

  public IdMatcher(final int[] values) {
    // For small amount of values, just brute force
    this.values = values;
    if (values.length == 0) {
      matcher = (value) -> {
        return true;
      };
    } else if (values.length == 1) {
      final int v = values[0];
      matcher = (value) -> {
        //log.debug("Testing for match with {}=={}", value, v);
        return value == v;
      };
    } else if (values.length <= 3) {
      matcher = (value) -> {
        for (int v : values) {
          if (v == value) return true;
        }
        return false;
      };
    } else {
      final TIntSet matchSet = new TIntHashSet(values);
      matcher = matchSet::contains;
    }
  }

  public boolean match(final int v) {
    return matcher.test(v);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    IdMatcher other = (IdMatcher) obj;
    if (!Arrays.equals(values, other.values)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "IdMatcher [values=" + Arrays.toString(values) + "]";
  }
}
