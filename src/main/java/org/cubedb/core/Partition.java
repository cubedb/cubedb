package org.cubedb.core;

import java.io.File;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;

import com.esotericsoftware.kryo.KryoSerializable;

public interface Partition extends KryoSerializable{
	void insert(DataRow data);
	//Map<String,Map<String, Map<String, Long>>> get(List<Filter> filters);
	SearchResult get(List<Filter> filters);
	int getNumRecords();
	Map<String, Object> getStats();
	Stream<DataRow> asDataRowStream();
	boolean optimize();
}
