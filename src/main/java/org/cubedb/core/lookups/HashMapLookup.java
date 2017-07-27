package org.cubedb.core.lookups;

import org.cubedb.core.Constants;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HashMapLookup implements Lookup {

  @Override
  public String toString() {
    return "HashMapLookup [keys=" + keys + "]";
  }

  public static final Logger log = LoggerFactory.getLogger(HashMapLookup.class);
  private TObjectIntMap<String> m;
  private List<String> keys;

  public HashMapLookup(boolean needNull) {
    init(needNull);
  }

  protected void init(boolean needNull) {
    this.m = new TObjectIntHashMap<String>(8);
    this.keys = new ArrayList<String>(8);
    if (needNull) {
      this.keys.add(Constants.NULL_VALUE);
      this.m.put(Constants.NULL_VALUE, 0);
    }
  }

  public HashMapLookup() {
    this(true);
  }

  @Override
  public synchronized int getValue(String key) {
    if (key == null) key = Constants.NULL_VALUE;
    int val;
    if (!m.containsKey(key)) {
      val = m.size();
      Objects.requireNonNull(key);
      m.put(key, val);
      keys.add(key);
    } else {
      val = m.get(key);
    }
    return val;
  }

  @Override
  public boolean containsValue(String key) {
    return m.containsKey(key);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((m == null) ? 0 : m.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    HashMapLookup other = (HashMapLookup) obj;
    if (m == null) {
      if (other.m != null) return false;
    } else if (!m.equals(other.m)) return false;
    return true;
  }

  @Override
  public int size() {
    return this.m.size();
  }

  @Override
  public String[] getKeys() {
    int keysCount = this.size();
    String[] keys = new String[keysCount];
    for (int i = 0; i < keysCount; i++) {
      keys[i] = this.keys.get(i);
    }
    return keys;
  }

  @Override
  public String getKey(int id) {
    return this.keys.get(id);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    int numKeys = this.keys.size();
    output.writeInt(numKeys);
    for (int i = 0; i < numKeys; i++) output.writeString(this.keys.get(i));
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int numStrings = input.readInt();
    init(false);
    for (int i = 0; i < numStrings; i++) {
      this.getValue(input.readString());
    }
  }
}
