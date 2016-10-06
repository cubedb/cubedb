package org.cubedb.core;

import java.util.List;
import java.util.Map;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResultRow;

public interface MultiCube {
	public void insert(List<DataRow> data);

	public Map<SearchResultRow, Long> get(String cubeName, String fromPartition, String toPartition,
			List<Filter> filters);

	public Map<SearchResultRow, Long> get(String cubeName, int lastNum,
			List<Filter> filters);
	
	public boolean hasCube(String cubeName);
	
	public void save(String path);
	public int deleteCube(String cubeName, String fromPartition, String toPartition);
	public int deleteCube(String cubeName, int keepLastN);
	public int deleteCube(int keepLastN);
	public int deleteCube(String fromPartition, String toPartition);

	public String getPath();

	void load(String path);
	
	Map<String, Object> getStats();
	
	public void saveAsJson(String path);
	
}
