package org.cubedb.core;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;

import com.esotericsoftware.kryo.KryoSerializable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface Partition extends KryoSerializable {

  void insert(DataRow data);

  SearchResult get(List<Filter> filters, String groupFieldName);

  int getNumRecords();

  Map<String, Object> getStats();

  Map<String, Set<String>> getFieldToValues();

  Stream<DataRow> asDataRowStream();

  boolean optimize();
}
