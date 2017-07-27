package org.cubedb.core.lookups;

import com.esotericsoftware.kryo.KryoSerializable;

public interface Lookup extends KryoSerializable {

  public int getValue(String key);

  public boolean containsValue(String key);

  public int size();

  public String[] getKeys();

  public String getKey(int id);
}
