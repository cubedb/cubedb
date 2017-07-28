package org.cubedb.core;

import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public interface Cube extends BaseCubeInterface {

  Map<GroupedSearchResultRow, Long> get(
      String fromPartition, String toPartition, List<Filter> filters, String groupBy);

  Map<GroupedSearchResultRow, Long> get(int lastRange, List<Filter> filters, String groupBy);

  void deletePartition(String partitionName);

  TreeSet<String> getPartitions(String from, String to);

  void saveAsJson(String saveFileName, String cubeName) throws IOException;
}
