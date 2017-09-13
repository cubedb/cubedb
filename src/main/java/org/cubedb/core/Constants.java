package org.cubedb.core;

public class Constants {

  public static final String DEFAULT_PARTITION_NAME = "p";
  public static int MAX_DIMENSIONS_SIZE = 256;
  public static int INITIAL_PARTITION_SIZE = 4096; //aka buffer size
  public static final int START_TINY_SIZE = 256;
  public static String NULL_VALUE = "null";

  // Stats name constants
  public static final String STATS_COLUMN_SIZE = "columnSizeBytes";
  public static final String STATS_METRIC_SIZE = "metricSizeBytes";
  public static final String STATS_COLUMN_BLOCKS = "columnBlocks";
  public static final String STATS_METRIC_BLOCKS = "metricBlocks";
  public static final String STATS_LOOKUP_SIZE = "lookupSizeBytes";
  public static final String TOTAL_SIZE = "totalSizeBytes";
  public static final String STATS_NUM_COLUMNS = "numColumns";
  public static final String STATS_NUM_RECORDS = "numRecords";
  public static final String STATS_NUM_PARTITIONS = "numPartitions";
  public static final String STATS_LAST_INSERT = "secSinceLastInsert";
  public static final String STATS_LAST_RECORD_APPEND = "secSinceLastRecordAppend";
  public static final String STATS_LAST_SAVE = "secSinceLastSave";
  public static final String STATS_CARDINALITIES = "columnCardinalities";
  public static final String STATS_NUM_CUBES = "numCubes";
  public static final String STATS_NUM_LARGE_BLOCKS = "numLargeBlocks";
  public static final String STATS_IS_READONLY_PARTITION = "isReadOnly";
  public static final String STATS_NUM_READONLY_PARTITIONS = "numReadOnlyPartitions";
  public static final String STATS_CUBE_FIELD_TO_VALUE_NUM = "cubeFieldToValueNum";
  public static final String STATS_CUBE_MAX_PARTITION = "cubeMaxPartition";
  public static final String STATS_CUBE_MIN_PARTITION = "cubeMinPartition";

  public static long KEY_MAP_TTL =
      1 * 24 * 3600 * 1000; // 1 day, milliseconds. Please note it is not final.
  public static final boolean sendStats = true;
}
