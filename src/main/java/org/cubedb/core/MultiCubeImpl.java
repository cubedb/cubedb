package org.cubedb.core;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.GroupedSearchResultRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Map<String, List<DataRow>> groupedData =
        data.stream().collect(Collectors.groupingBy(DataRow::getCubeName));
    for (Entry<String, List<DataRow>> cubeEntry : groupedData.entrySet()) {
      final String cubeName = cubeEntry.getKey();
      final List<DataRow> cubeData = cubeEntry.getValue();
      cubes.computeIfAbsent(cubeName, this::createNewCube).insert(cubeData);
    }
  }

  @Override
  public Map<GroupedSearchResultRow, Long> get(
      String cubeName, String fromPartition, String toPartition, List<Filter> filters) {
    return get(cubeName, fromPartition, toPartition, filters, null);
  }

  @Override
  public Map<GroupedSearchResultRow, Long> get(
      String cubeName,
      String fromPartition,
      String toPartition,
      List<Filter> filters,
      String groupBy) {
    Cube c = cubes.get(cubeName);
    if (c != null) {
      return c.get(fromPartition, toPartition, filters, groupBy);
    } else {
      return new HashMap<>();
    }
  }

  @Override
  public boolean hasCube(String cubeName) {
    return cubes.containsKey(cubeName);
  }

  @Override
  public Cube getCube(String cubeName) {
    return cubes.get(cubeName);
  }

  @Override
  public Map<GroupedSearchResultRow, Long> get(String cubeName, int lastNum, List<Filter> filters) {
    return get(cubeName, lastNum, filters, null);
  }

  @Override
  public Map<GroupedSearchResultRow, Long> get(
      String cubeName, int lastNum, List<Filter> filters, String groupBy) {
    Cube c = cubes.get(cubeName);
    if (c != null) {
      return c.get(lastNum, filters, groupBy);
    } else {
      return new HashMap<>();
    }
  }

  @Override
  public void save(String path) throws IOException {
    save(path, false);
  }

  public synchronized void save(String path, boolean asJson) throws IOException {
    File targetDirectory = new File(path);
    if (targetDirectory.exists() && targetDirectory.isFile()) {
      log.error("Attempting to save to directory");
      throw new InvalidParameterException("Path specified is a file");
    }
    if (!targetDirectory.exists()) {
      targetDirectory.mkdirs();
    }

    File tmpDirectory = Files.createTempDirectory(targetDirectory.toPath(), ".tmp").toFile();
    log.info("Saving temporarily to {}", tmpDirectory.getAbsolutePath());
    cubes
        .entrySet()
        .stream()
        .parallel()
        .forEach(
            cube -> {
              String tmpPath = tmpDirectory.getAbsolutePath()
                               + "/" + cube.getKey()
                               + (asJson ? ".gz" : ".snappy");
              try {
                if (asJson) {
                  cube.getValue().saveAsJson(tmpPath, cube.getKey());
                } else {
                  cube.getValue().save(tmpPath);
                }
                File tmpFile = new File(tmpPath);
                File targetFile = new File(targetDirectory, tmpFile.getName());
                tmpFile.renameTo(targetFile);
              } catch (FileNotFoundException e) {
                log.error("File not found: {}", tmpPath);
                log.error("error", e);
              } catch (IOException e) {
                log.error("Could not save {} in {}", cube.getKey(), tmpPath);
                e.printStackTrace();
              }
            });
    tmpDirectory.delete();
    lastSaveTsMs = System.currentTimeMillis();
  }

  @Override
  public void load(String path) {
    long t0 = System.currentTimeMillis();

    File p = new File(path);
    if (p.exists()) {
      if (p.isFile()) {
        log.error("Attempting to load from directory");
        throw new InvalidParameterException("Path specified is a file");
      }

      for (File cubeFile : p.listFiles()) {
        log.info("Loading from file {}", cubeFile.getAbsolutePath());
        String cubeName = cubeFile.getName().replace(".gz", "").replace(".snappy", "");
        Cube c = createNewCube(partitionColumnName);
        try {
          c.load(cubeFile.getAbsolutePath());
          cubes.put(cubeName, c);
        } catch (IOException e) {
          log.error("Could no load cube {}", cubeName);
        }
      }
    } else {
      log.warn("Save path {} does not exist. It will be created next time when saving", path);
    }
    long t1 = System.currentTimeMillis();
    log.info("Loading time: {}ms", t1 - t0);
  }

  @Override
  public int deleteCube(String cubeName, String fromPartition, String toPartition) {
    int deletedCount = 0;
    Cube c = cubes.get(cubeName);
    if (fromPartition == null && toPartition == null) {
      deletedCount += cubes.get(cubeName).getPartitions(null, null).size();
      cubes.remove(cubeName);

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
    Cube c = cubes.get(cubeName);
    int deletedCount = 0;
    if (c != null) {
      TreeSet<String> partitions = c.getPartitions(null, null);
      final int indexToDelete = partitions.size() - keepLastN;
      int i = 0;
      for (String partitionName : partitions) {
        if (i < indexToDelete) {
          c.deletePartition(partitionName);
          deletedCount++;
        } else break;
        i++;
      }
      if (i > 0) {
        log.debug("Removed {} partitions from {}", i, cubeName);
      }
    }
    return deletedCount;
  }

  @Override
  public int deleteCube(int keepLastN) {
    int c = 0;
    for (String cubeName : cubes.keySet()) {
      c += deleteCube(cubeName, keepLastN);
    }
    return c;
  }

  @Override
  public int deleteCube(String fromPartition, String toPartition) {
    int c = 0;
    for (String cubeName : cubes.keySet()) {
      c += deleteCube(cubeName, fromPartition, toPartition);
    }
    return c;
  }

  @Override
  public String getPath() {
    return savePath;
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Map<String, Object>> partitionStats =
        cubes
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getStats()));
    Map<String, Object> out = new HashMap<String, Object>();
    out.put("cubeStats", partitionStats);
    out.put(
        Constants.STATS_COLUMN_SIZE,
        partitionStats
        .values()
        .stream()
        .mapToLong(e -> (Long) e.get(Constants.STATS_COLUMN_SIZE))
        .sum());
    out.put(
        Constants.STATS_METRIC_SIZE,
        partitionStats
        .values()
        .stream()
        .mapToLong(e -> (Long) e.get(Constants.STATS_METRIC_SIZE))
        .sum());
    out.put(
        Constants.STATS_COLUMN_BLOCKS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_COLUMN_BLOCKS))
        .sum());
    out.put(
        Constants.STATS_METRIC_BLOCKS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_METRIC_BLOCKS))
        .sum());
    out.put(
        Constants.STATS_NUM_RECORDS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_RECORDS))
        .sum());
    out.put(
        Constants.STATS_NUM_PARTITIONS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_PARTITIONS))
        .sum());
    out.put(
        Constants.STATS_NUM_LARGE_BLOCKS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_LARGE_BLOCKS))
        .sum());
    out.put(
        Constants.STATS_NUM_READONLY_PARTITIONS,
        partitionStats
        .values()
        .stream()
        .mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_READONLY_PARTITIONS))
        .sum());
    out.put(Constants.STATS_NUM_CUBES, partitionStats.size());
    return out;
  }

  public int optimize() {
    return cubes.values().stream().mapToInt(Cube::optimize).sum();
  }

  @Override
  public void saveAsJson(String path) throws IOException {
    save(path, true);
  }
}
