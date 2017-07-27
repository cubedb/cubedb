package org.cubedb.core;

import org.cubedb.core.beans.DataRow;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Created by krash on 28.06.17. */
public interface BaseCubeInterface {
  /** Appending data to cube. */
  public void insert(List<DataRow> data);

  /** Performs internal optimizations + data free. */
  public int optimize();

  /** Dump content to specified path. */
  public void save(String path) throws IOException;

  /** Initialize cube with data, stored at path. */
  public void load(String path) throws IOException;

  /** Retrieve internal state of cube. */
  public Map<String, Object> getStats();
}
