package org.cubedb.core;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;

import com.esotericsoftware.kryo.KryoSerializable;

public interface Partition extends KryoSerializable {

  void insert(DataRow data);

  SearchResult get(List<Filter> filters, String groupFieldName);

  int getNumRecords();

  Map<String, Object> getStats();

  Stream<DataRow> asDataRowStream();

  boolean optimize();
}
