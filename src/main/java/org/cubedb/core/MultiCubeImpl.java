package org.cubedb.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiCubeImpl implements MultiCube {
	public static final Logger log = LoggerFactory.getLogger(MultiCubeImpl.class);
	protected long lastSaveTsMs;
	protected boolean isCurrentlySavingOrLoading;
	protected String savePath;

	Map<String, Cube> cubes = new HashMap<String, Cube>();

	final String partitionColumnName = Constants.DEFAULT_PARTITION_NAME;

	public MultiCubeImpl(String savePath) {
		this.savePath = savePath;
		this.isCurrentlySavingOrLoading = false;
	}

	private Cube createNewCube(String cubeName) {
		Cube c = new CubeImpl(partitionColumnName);
		return c;
	}

	@Override
	public synchronized void insert(List<DataRow> data) {
		Map<String, List<DataRow>> groupedData = data.stream().collect(Collectors.groupingBy(DataRow::getCubeName));
		for (Entry<String, List<DataRow>> cubeEntry : groupedData.entrySet()) {
			final String cubeName = cubeEntry.getKey();
			final List<DataRow> cubeData = cubeEntry.getValue();
			cubes.computeIfAbsent(cubeName, this::createNewCube).insert(cubeData);
		}
	}

	@Override
	public Map<SearchResultRow, Long> get(String cubeName, String fromPartition, String toPartition,
			List<Filter> filters) {
		Cube c = this.cubes.get(cubeName);
		if (c != null) {
			return c.get(fromPartition, toPartition, filters);
		} else {
			return new HashMap<SearchResultRow, Long>();
		}

	}

	@Override
	public boolean hasCube(String cubeName) {
		return this.cubes.containsKey(cubeName);
	}

	@Override
	public Map<SearchResultRow, Long> get(String cubeName, int lastNum, List<Filter> filters) {
		Cube c = this.cubes.get(cubeName);
		if (c != null) {
			return c.get(lastNum, filters);
		} else {
			return new HashMap<SearchResultRow, Long>();
		}
	}

	@Override
	public void save(String path) {
		this.save(path, false);
	}
	
	public void save(String path, boolean asJson) {
		if (this.isCurrentlySavingOrLoading) {
			log.warn("Process of saving or loading is currently in progress.");
			return;
		}
		this.isCurrentlySavingOrLoading = true;
		File p = new File(path);
		if (p.exists() && p.isFile()) {
			log.error("Attempting to save to directory");
			throw new InvalidParameterException("Path specifid is a file");
		} else {
			p.mkdirs();
		}
		File destination;
		try {
			destination = Files.createTempDirectory(p.toPath(), ".tmp").toFile();
			log.info("Saving temporarily to {}", destination.getAbsolutePath());
			for (Entry<String, Cube> e : this.cubes.entrySet()) {
				String saveFileName = destination.getAbsolutePath() + "/" + e.getKey() + ".gz";
				try {
					if(asJson)
						e.getValue().save(saveFileName);
					else
						e.getValue().saveAsJson(saveFileName, e.getKey());
					File f = new File(saveFileName);
					File newF = new File(p, f.getName());
					f.renameTo(newF);

				} catch (FileNotFoundException e1) {
					log.error("File not found: {}", saveFileName);
					log.error("error", e1);
				} catch (IOException e1) {
					log.error("Could not save {} in {}", e.getKey(), saveFileName);
					e1.printStackTrace();
				}
			}
			destination.delete();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		lastSaveTsMs = System.currentTimeMillis();
		this.isCurrentlySavingOrLoading = false;
	}

	@Override
	public void load(String path) {
		if (this.isCurrentlySavingOrLoading) {
			log.warn("Process of saving or loading is currently in progress.");
			return;
		}
		this.isCurrentlySavingOrLoading = true;
		File p = new File(path);
		if (p.exists()) {
			if (p.isFile()) {
				log.error("Attempting to load from directory");
				throw new InvalidParameterException("Path specified is a file");
			}
			for (File cubeFile : p.listFiles()) {
				log.info("Loading from file {}", cubeFile.getAbsolutePath());
				String cubeName = cubeFile.getName().replace(".gz", "");
				Cube c = createNewCube(partitionColumnName);
				try {
					c.load(cubeFile.getAbsolutePath());
					this.cubes.put(cubeName, c);
				} catch (IOException e) {
					log.error("Could no load cube {}", cubeName);
				}

			}
		} else {
			log.warn("Save path {} does not exist. It will be created next time when saving", path);
		}
		this.isCurrentlySavingOrLoading = false;
	}

	@Override
	public int deleteCube(String cubeName, String fromPartition, String toPartition) {
		int deletedCount = 0;
		Cube c = this.cubes.get(cubeName);
		if (fromPartition == null && toPartition == null) {
			deletedCount += this.cubes.get(cubeName).getPartitions(null, null).size();
			this.cubes.remove(cubeName);

		} else {
			if (c != null) {
				for (String partitionName : c.getPartitions(fromPartition, toPartition)) {
					deletedCount++;
					c.deletePartition(partitionName);
				}
			}
		}
		return deletedCount;

	}

	@Override
	public int deleteCube(String cubeName, int keepLastN) {
		Cube c = this.cubes.get(cubeName);
		int deletedCount = 0;
		if (c != null) {
			TreeSet<String> partitions = c.getPartitions(null, null);
			final int indexToDelete = partitions.size() - keepLastN;
			int i = 0;
			for (String partitionName : partitions) {
				if (i < indexToDelete) {
					c.deletePartition(partitionName);
					deletedCount++;
				} else
					break;
				i++;
			}
			if(i>0){
				log.debug("Removed {} partitions from {}", i, cubeName);
			}
		}
		return deletedCount;

	}

	@Override
	public int deleteCube(int keepLastN) {
		int c = 0;
		for (String cubeName : this.cubes.keySet()) {
			c += this.deleteCube(cubeName, keepLastN);
		}
		return c;
	}
	
	@Override
	public int deleteCube(String fromPartition, String toPartition) {
		int c = 0;
		for (String cubeName : this.cubes.keySet()) {
			c += this.deleteCube(cubeName, fromPartition, toPartition);
		}
		return c;
	}

	@Override
	public String getPath() {
		return this.savePath;
	}

	@Override
	public Map<String, Object> getStats() {
		Map<String, Map<String, Object>> partitionStats = this.cubes.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getStats()));
		Map<String, Object> out = new HashMap<String, Object>();
		out.put("cubeStats", partitionStats);
		// this.columns.values().stream().mapToLong(Column::size).sum();
		out.put(Constants.STATS_COLUMN_SIZE,
				partitionStats.values().stream().mapToLong(e -> (Long) e.get(Constants.STATS_COLUMN_SIZE)).sum());
		out.put(Constants.STATS_METRIC_SIZE,
				partitionStats.values().stream().mapToLong(e -> (Long) e.get(Constants.STATS_METRIC_SIZE)).sum());
		out.put(Constants.STATS_COLUMN_BLOCKS,
				partitionStats.values().stream().mapToInt(e -> (Integer) e.get(Constants.STATS_COLUMN_BLOCKS)).sum());
		out.put(Constants.STATS_METRIC_BLOCKS,
				partitionStats.values().stream().mapToInt(e -> (Integer) e.get(Constants.STATS_METRIC_BLOCKS)).sum());
		out.put(Constants.STATS_NUM_RECORDS,
				partitionStats.values().stream().mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_RECORDS)).sum());
		out.put(Constants.STATS_NUM_PARTITIONS,
				partitionStats.values().stream().mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_PARTITIONS)).sum());
		out.put(Constants.STATS_NUM_LARGE_BLOCKS, partitionStats.values().stream()
				.mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_LARGE_BLOCKS)).sum());
		out.put(Constants.STATS_NUM_CUBES, partitionStats.size());
		return out;
	}
	
	@Override
	public void saveAsJson(String path)
	{
		this.save(path, true);
	}

}
