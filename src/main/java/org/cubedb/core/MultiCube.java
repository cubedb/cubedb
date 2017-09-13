package org.cubedb.core;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MultiCube extends BaseCubeInterface {
  public void insert(List<DataRow> data);

  public Map<GroupedSearchResultRow, Long> get(
      String cubeName, String fromPartition, String toPartition, List<Filter> filters);

  public Map<GroupedSearchResultRow, Long> get(
      String cubeName,
      String fromPartition,
      String toPartition,
      List<Filter> filters,
      String groupBy);

  public Map<GroupedSearchResultRow, Long> get(String cubeName, int lastNum, List<Filter> filters);

  public Map<GroupedSearchResultRow, Long> get(
      String cubeName, int lastNum, List<Filter> filters, String groupBy);

  public boolean hasCube(String cubeName);

  public Cube getCube(String cubeName);

  public int deleteCube(String cubeName, String fromPartition, String toPartition);

  public int deleteCube(String cubeName, int keepLastN);

  public int deleteCube(int keepLastN);

  public int deleteCube(String fromPartition, String toPartition);

  public String getPath();

  public void saveAsJson(String path) throws IOException;
}
