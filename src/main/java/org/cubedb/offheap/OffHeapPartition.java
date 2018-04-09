package org.cubedb.offheap;

import org.cubedb.core.Column;
import org.cubedb.core.Constants;
import org.cubedb.core.KeyMap;
import org.cubedb.core.Metric;
import org.cubedb.core.Partition;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.counters.CounterContainer;
import org.cubedb.core.lookups.HashMapLookup;
import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.tiny.TinyColumn;
import org.cubedb.core.tiny.TinyMetric;
import org.cubedb.core.tiny.TinyUtils;
import org.cubedb.offheap.matchers.IdMatcher;
import org.cubedb.utils.CubeUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OffHeapPartition implements Partition {

  private static final int FAKE_GROUP_VALUE_ID = 0;

  // Field name -> field value <--> value ID
  protected Map<String, Lookup> lookups;
  // Field name <--> fieldId
  protected Lookup fieldLookup;
  // Metric field name <--> metric field ID
  protected Lookup metricLookup;
  // Field name (== column name) -> a data column
  protected Map<String, Column> columns;
  // Cumulative partition data metrics: metric name -> metric value.
  protected Map<String, Metric> metrics;
  // Number of rows in the partition.
  protected int size;
  protected KeyMap map;
  private static final Logger log = LoggerFactory.getLogger(OffHeapPartition.class);
  protected long lastInsertTs;
  protected long lastAppendTs;
  protected long startupTs;
  protected long lastSaveTs;
  protected final ReadWriteLock lock;

  public OffHeapPartition() {
    // log.debug("Initializing Partition");
    lookups = new HashMap<String, Lookup>(5);
    fieldLookup = new HashMapLookup(false);
    columns = new HashMap<String, Column>(5);
    metrics = new HashMap<String, Metric>(1);
    metricLookup = new HashMapLookup(false);
    lock = new ReentrantReadWriteLock(false);
  }

  protected void addColumn(String columnName) {
    Column col = new TinyColumn(size);
    columns.putIfAbsent(columnName, col);
  }

  protected void addMetric(String metricName) {
    Metric m = new TinyMetric(size);
    metrics.putIfAbsent(metricName, m);
    metricLookup.getValue(metricName);
  }

  protected void createMap(int fieldsLength) {
    // map = new MapDBKeyMap(size, fieldsLength);
    map = new BOHKeyMap(size, fieldsLength);
  }

  protected void initializeMap() {
    log.debug("Re-Initializing map");
    final Column[] fields = new Column[fieldLookup.getKeys().length];
    for (int i = 0; i < fields.length; i++) {
      String fieldKey = fieldLookup.getKey(i);
      fields[i] = columns.get(fieldKey);
    }

    createMap(fields.length);

    byte[] b = new byte[fields.length * Short.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(b);
    int j = 0;
    for (int i = 0; i < size; i++) {
      bb.clear();
      for (j = 0; j < fields.length; j++) {
        Column c = fields[j];
        short val = (short) c.get(i);
        bb.putShort(val);
      }
      map.put(b, i);
    }
  }

  @Override
  public boolean optimize() {
    long ts = System.currentTimeMillis();
    if (ts - lastInsertTs > Constants.KEY_MAP_TTL) {
      map = null;
      return true;
    }
    return false;
  }

  // TODO: refactor to accept int[], long[]
  protected void insertFields(short[] fields, Map<String, Long> newMetrics) {
    ByteBuffer buf = ByteBuffer.allocate(fields.length * Short.BYTES);
    buf.clear();
    for (short f : CubeUtils.cutZeroSuffix(fields)) buf.putShort(f);
    byte[] bytes = buf.array();
    if (map == null) {
      initializeMap();
    }
    Integer index = map.get(bytes);

    // 1. Check if there are any new metrics
    for (String metricName : newMetrics.keySet()) {
      if (!metrics.containsKey(metricName)) {
        // log.info("New metric {} found", metricName);
        if (size != 0)
          throw new RuntimeException("Adding new metrics on fly is not implemented yet");
        addMetric(metricName);
      }
    }

    // 2. Check if this combination of fields has ever existed.
    // If never existed, create one.
    if (index == null) {
      // log.debug("Inserting new combination of dimensions into
      // partition");
      index = new Integer(size);
      map.put(bytes, index);
      for (int i = 0; i < fields.length; i++) {
        String fieldName = fieldLookup.getKey(i);
        // log.debug("Writing {}={} to buffers", fieldName, fields[i]);
        Column col = columns.get(fieldName);
        if (col.isTiny() && col.getNumRecords() > Constants.INITIAL_PARTITION_SIZE) {
          // log.debug("There are {} records, converting TinyColumn {}
          // to OffHeap", col.getNumRecords(), fieldName);
          col = TinyUtils.tinyColumnToOffHeap((TinyColumn) col);
          columns.put(fieldName, col);
        }
        col.append(fields[i]);
      }
      for (Entry<String, Metric> e : metrics.entrySet()) {
        Metric m = e.getValue();
        String metricName = e.getKey();
        if (m.isTiny() && m.getNumRecords() > Constants.INITIAL_PARTITION_SIZE) {
          // log.debug("Converting TinyMetric {} to OffHeap",
          // metricName);
          m = TinyUtils.tinyMetricToOffHeap((TinyMetric) m);
          metrics.put(metricName, m);
        }
        m.append(0L);
      }
      lastAppendTs = System.currentTimeMillis();
      size++;
    }

    // 3. Increment metrics by values supplied.
    for (Entry<String, Metric> e : metrics.entrySet()) {
      Long c = newMetrics.get(e.getKey()).longValue();
      if (c != null) {
        Metric metric = e.getValue();
        metric.incrementBy(index.intValue(), c.longValue());
      }
    }
  }

  protected void addNewFields(DataRow row) {
    Set<String> newFields = new HashSet<String>(row.getFields().keySet());
    for (String f : fieldLookup.getKeys())
      if (row.getFields().containsKey(f)) newFields.remove(f);

    if (newFields.size() > 0) {
      for (String f : newFields) {
        final int newColumnIndex = fieldLookup.getValue(f);
        lookups.put(f, new HashMapLookup());
        columns.put(f, new TinyColumn(size));
      }
      insertImpl(row);
    }
  }

  protected void insertImpl(DataRow row) {
    short[] fields = new short[fieldLookup.size()];
    int i = 0;

    int insertedFieldCount = 0;
    for (String fieldName : fieldLookup.getKeys()) {
      String value = row.getFields().get(fieldName);
      int valueIndex = 0;
      if (value != null || row.getFields().containsKey(fieldName)) {
        insertedFieldCount++;
        valueIndex = lookups.get(fieldName).getValue(value != null ? value : Constants.NULL_VALUE);
        // log.debug("Index for value {}.{} is {}", fieldName, value,
        // valueIndex);
      }
      fields[i] = (short) valueIndex;
      i++;
    }

    // If a new field was detected, rebuild the whole lookup table
    if (insertedFieldCount != row.getFields().size()) {
      // log.info("Inserted {} fields, but the row has {} fields",
      // insertedFieldCount, row.getFields().size());
      addNewFields(row);
    } else {
      insertFields(fields, row.getCounters());
    }
  }

  @Override
  public void insert(DataRow row) {
    insertImpl(row);
    lastInsertTs = System.currentTimeMillis();
  }

  public void insertData(List<DataRow> data) {
    log.info("Inserting {} rows", data.size());
    long t0 = System.nanoTime();
    data.forEach(this::insert);
    long t1 = System.nanoTime() - t0;
    long rowsPerSecond = 1000000000L * data.size() / t1;
    log.info(
        "Took {}ms to insert {} rows, {}μs/row",
        t1 / 1000 / 1000,
        data.size(),
        t1 / data.size() / 1000);
    log.info("That is {} rows/sec", rowsPerSecond);
  }

  protected Map<String, IdMatcher> transformFiltersToMatchers(List<Filter> filters) {
    // log.debug("List of filters: {}", filters);
    Map<String, IdMatcher> fieldNameToMatchers = new HashMap<String, IdMatcher>();
    Map<String, Set<String>> filtersByColumn = new HashMap<String, Set<String>>();
    for (String columnName : columns.keySet()) {
      filtersByColumn.put(columnName, new HashSet<String>());
    }

    // For every filtered column pick extract possible column value
    for (Filter filter : filters) {
      Set<String> columnValues = filtersByColumn.get(filter.getField());
      Collections.addAll(columnValues, filter.getValues());
    }

    // Convert possible column values to id-based column value matchers
    for (Entry<String, Set<String>> e : filtersByColumn.entrySet()) {
      String fieldName = e.getKey();
      Lookup valueIdLookup = lookups.get(fieldName);
      int[] valueIdList = e.getValue().stream().mapToInt(valueIdLookup::getValue).toArray();
      if (valueIdList.length > 0) {
        fieldNameToMatchers.put(fieldName, new IdMatcher(valueIdList));
      }
    }
    // log.debug("Resulting id matchers: {}", fieldNameToMatchers);
    return fieldNameToMatchers;
  }

  protected Column[] getColumnsAsArray() {
    final Column[] columnsArray = new Column[columns.size()];
    for (Entry<String, Column> e : columns.entrySet()) {
      String fieldName = e.getKey();
      int fieldId = fieldLookup.getValue(fieldName);
      columnsArray[fieldId] = e.getValue();
    }
    return columnsArray;
  }

  @Override
  public SearchResult get(List<Filter> filters, String groupFieldName) {
    // log.debug("Starting search");
    final long t0 = System.nanoTime(); // debug purposes
    final int curSize = size; // current max index of rows in the db

    // a field to use for result grouping
    final boolean doFieldGrouping = groupFieldName != null;

    /*
     * If the grouping field does not exist in the partition - just return
     * en empty result.
     */
    if (doFieldGrouping && !fieldLookup.containsValue(groupFieldName)) {
      log.warn(
          String.format("Grouping column %s does not exist in this partition", groupFieldName));
      return SearchResult.buildEmpty(metrics.keySet());
    }

    /*
     * If filters require non-existing columns we can skip the search
     * altogether.
     *
     * NOTE: also removing null value filters for non-existant columns as a non-existant column
     * is considered to have a null value.
     */
    filters =
        filters
            .stream()
            .filter(f -> columns.containsKey(f.getField()) || !f.isNullValueFilter())
            .collect(Collectors.toList());
    if (!checkAllFilterColumnsExist(filters)) {
      return SearchResult.buildEmpty(metrics.keySet());
    }

    final int groupFieldId;
    // field names -> (column value id -> (group value id -> (metric index
    // -> counter)))
    final CounterContainer sideCounterCountainer =
        new CounterContainer(fieldLookup, metricLookup, lookups);

    /*
     * When field grouping is *not required* we basically just use a
     * single-value array with a zero id for the groupValueId array.
     */
    if (doFieldGrouping) {
      groupFieldId = fieldLookup.getValue(groupFieldName);
      sideCounterCountainer.initGroupedSideCounters(groupFieldName);
    } else {
      groupFieldId = FAKE_GROUP_VALUE_ID;
      sideCounterCountainer.initSideCounters();
    }

    // creating an empty result set, with id's
    final String[] metricNames = metricLookup.getKeys();
    // metric name -> group value id -> counter
    int groupFieldValueCount = doFieldGrouping ? lookups.get(groupFieldName).size() : 1;
    final long[][] totalCounters = new long[metricNames.length][groupFieldValueCount];

    final Column[] columns = getColumnsAsArray();

    int matchCount = 0; // Debug variable
    final long t2; // these are for time measurement (debug purposes only)
    final long t3;

    // creating a map of matchers based on filter

    /*
     * these fields are mostly primitive array representations of what
     * previously was stored in Objects and HashMaps
     */

    /*
     * bitmask for filter matches
     */
    final boolean[] columnMatches = new boolean[fieldLookup.size()];

    /*
     * values of measures
     */
    final long[] metricValues = new long[metricNames.length];

    /*
     * values of columns. Id's only, no real string values.
     */
    final int[] columnValues = new int[fieldLookup.size()];

    /*
     * Fast representation of filters. IdMatcher means we are doing only
     * equality checking, no fancy >, <, !=, etc.
     */
    final IdMatcher[] matchersArray = new IdMatcher[fieldLookup.size()];
    final Metric[] metricsArray = new Metric[metricLookup.size()];

    /*
     * Filters are specification of criterias using strings. Here they are
     * transformed to an efficient representation, functions that match
     * using integer ids
     */
    final Map<String, IdMatcher> matchers = transformFiltersToMatchers(filters);

    for (Entry<String, IdMatcher> e : matchers.entrySet()) {
      int fieldId = fieldLookup.getValue(e.getKey());
      matchersArray[fieldId] = e.getValue();
    }

    /*
     * Here we get transform Map of metrics to an array of metrics. Note
     * that in 99% of cases there is only one metric
     */
    for (Entry<String, Metric> e : metrics.entrySet()) {
      int fieldId = metricLookup.getValue(e.getKey());
      metricsArray[fieldId] = e.getValue();
    }

    /*
     * Here starts the brute-force scanning. Operations in this block have
     * to be as fast as possible
     */

    final int fieldLookupSize = fieldLookup.size();
    t2 = System.nanoTime();
    for (int i = 0; i < curSize; i++) {
      /*
       * Here we do not retrieve values of all columns. We only get those
       * which are related to a filter/matcher
       */
      for (int matcherId = 0; matcherId < matchersArray.length; matcherId++) {
        IdMatcher matcher = matchersArray[matcherId];
        final int valueId = columns[matcherId].get(i);
        columnValues[matcherId] = valueId;

        if (matcher != null) {
          columnMatches[matcherId] = matcher.match(valueId);
        } else {
          // If no matcher is set for this column we consider it matched
          columnMatches[matcherId] = true;
        }
      }

      /*
       * At least one of the filters was matched. We need this row
       */
      if (atLeastOneMatch(columnMatches)) {
        matchCount++;
        /*
         * We have a match! First, we retrieve the counters values for
         * this row
         */
        for (int mIndex = 0; mIndex < metricNames.length; mIndex++) {
          final long c = metricsArray[mIndex].get(i);
          metricValues[mIndex] = c;
        }

        /*
         * Find out the value of the field to group by for the i-th row.
         */
        final int groupFieldValueId =
            doFieldGrouping ? columns[groupFieldId].get(i) : FAKE_GROUP_VALUE_ID;

        /*
         * Then, check if all columns of a row match. Increase the
         * totalCounters if positive.
         */
        if (checkAllMatch(columnMatches)) {
          for (int mIndex = 0; mIndex < metricNames.length; mIndex++) {
            totalCounters[mIndex][groupFieldValueId] += metricValues[mIndex];
          }
        }

        /*
         * Last, retrieve the values of all columns. For each side, we
         * increment the side counter, but only when *other* side
         * filters match.
         */
        for (int fieldId = 0; fieldId < fieldLookupSize; fieldId++) {
          /*
           * We don't care if the current side filter is applied or
           * not - it should only influence *other* side filtering.
           */
          if (!checkOtherMatch(columnMatches, fieldId)) {
            continue;
          }

          final int columnValueId = columnValues[fieldId];
          for (int mIndex = 0; mIndex < metricNames.length; mIndex++) {
            sideCounterCountainer.add(
                fieldId, columnValueId, groupFieldValueId, mIndex, metricValues[mIndex]);
          }
        }
      }
    }
    t3 = System.nanoTime();

    final long t_pre_build = System.nanoTime();
    final SearchResult result =
        SearchResult.buildFromResultArray(
            sideCounterCountainer,
            totalCounters,
            doFieldGrouping,
            groupFieldName,
            lookups,
            fieldLookup,
            metricLookup);
    final long t1 = System.nanoTime();
    log.debug("Building result from array took {}ms", (t1 - t_pre_build) / 1000000.0);
    log.debug(
        "Got {} matches for the query in {}ms among {} rows",
        matchCount,
        (t1 - t0) / 1000000.0,
        curSize);
    if (curSize > 0 && (t3 - t2) > 0) {
      int rowsPerSecond = (int) (1000000000L * curSize / (t3 - t2));
      log.debug("Bruteforce search itself took {} ms", (t3 - t2) / 1000000.0);
      log.debug("Bruteforce search is {} rows/second", rowsPerSecond);
    }
    return result;
  }

  private boolean checkAllFilterColumnsExist(List<Filter> filters) {
    for (Filter filter : filters) {
      if (!columns.containsKey(filter.getField())) {
        return false;
      }
    }
    return true;
  }

  private boolean checkAllMatch(boolean[] matches) {
    for (int i = 0; i < matches.length; i++) {
      if (!matches[i]) {
        return false;
      }
    }
    return true;
  }

  private boolean checkOtherMatch(boolean[] matches, int side) {
    for (int i = 0; i < matches.length; i++) {
      if (!matches[i] && i != side) {
        return false;
      }
    }
    return true;
  }

  // TODO: maybe rewrite to a bitwise op
  private boolean atLeastOneMatch(boolean[] matches) {
    // boolean atLeastOneMatch = false;
    for (int i = 0; i < matches.length; i++) {
      if (matches[i]) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getNumRecords() {
    return size;
  }

  @Override
  public Map<String, Object> getStats() {
    long columnSize = columns.values().stream().mapToLong(Column::size).sum();
    long metricSize = metrics.values().stream().mapToLong(Metric::size).sum();
    int columnBlocks = columns.values().stream().mapToInt(Column::getNumBuffers).sum();
    int metricBLocks = metrics.values().stream().mapToInt(Metric::getNumBuffers).sum();
    long lookupSize = (long) (map != null ? map.size() : 0L) * columns.size() * Short.BYTES;
    Map<String, Object> stats = new HashMap<String, Object>();
    stats.put(Constants.STATS_COLUMN_SIZE, columnSize);
    stats.put(Constants.STATS_METRIC_SIZE, metricSize);
    stats.put(Constants.STATS_COLUMN_BLOCKS, columnBlocks);
    stats.put(Constants.STATS_METRIC_BLOCKS, metricBLocks);
    stats.put(Constants.STATS_LOOKUP_SIZE, lookupSize);
    stats.put(Constants.STATS_LAST_INSERT, lastInsertTs);
    stats.put(Constants.STATS_LAST_RECORD_APPEND, lastAppendTs);
    stats.put(Constants.STATS_NUM_RECORDS, size);
    stats.put(Constants.STATS_NUM_COLUMNS, columns.size());
    stats.put(
        Constants.STATS_NUM_LARGE_BLOCKS,
        metrics.values().stream().mapToInt(e -> e.isTiny() ? 0 : 1).sum()
        + columns.values().stream().mapToInt(e -> e.isTiny() ? 0 : 1).sum());
    stats.put(Constants.STATS_IS_READONLY_PARTITION, map == null);
    // stats.put(Constants.STATS_LAST_SAVE, lastInsertTs);
    lookups
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().size()));
    return stats;
  }

  @Override
  public Map<String, Set<String>> getFieldToValues() {
    return lookups
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey, e -> new HashSet<String>(Arrays.asList(e.getValue().getKeys()))));
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeInt(size);
    // add fieldlookup
    kryo.writeClassAndObject(output, fieldLookup);
    // add dimension lookups
    kryo.writeClassAndObject(output, lookups);
    // add metriclookup
    kryo.writeClassAndObject(output, metricLookup);
    // add columns
    kryo.writeClassAndObject(output, columns);
    // add metrics
    kryo.writeClassAndObject(output, metrics);
    lastSaveTs = System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void read(Kryo kryo, Input input) {
    size = input.readInt();
    fieldLookup = (Lookup) kryo.readClassAndObject(input);
    lookups = (Map<String, Lookup>) kryo.readClassAndObject(input);
    metricLookup = (Lookup) kryo.readClassAndObject(input);
    columns = (Map<String, Column>) kryo.readClassAndObject(input);
    metrics = (Map<String, Metric>) kryo.readClassAndObject(input);
    initializeMap();
  }

  protected Map<String, String> bytesToMap(byte[] in) {
    Map<String, String> out = new HashMap<String, String>();
    ByteBuffer b = ByteBuffer.wrap(in);
    for (int i = 0; i < fieldLookup.size(); i++) {
      String fieldName = fieldLookup.getKey(i);
      String fieldValue;
      if (b.remaining() > 0) {
        int fieldId = b.getShort();
        fieldValue = lookups.get(fieldName).getKey(fieldId);
        fieldValue = fieldValue.equals("null") ? null : fieldValue;
      } else fieldValue = null;
      out.put(fieldName, fieldValue);
    }
    return out;
  }

  protected Map<String, Long> metricsToMap(int offset) {
    Map<String, Long> out = new HashMap<String, Long>();
    for (int i = 0; i < metricLookup.size(); i++) {
      String metricName = metricLookup.getKey(i);
      Long metricValue = metrics.get(metricName).get(offset);
      out.put(metricName, metricValue);
    }
    return out;
  }

  @Override
  public Stream<DataRow> asDataRowStream() {
    if (map == null) initializeMap();
    return map.entrySet()
        .map(
            e -> {
              DataRow r = new DataRow();
              r.setFields(bytesToMap(e.getKey()));
              int offset = e.getValue();
              r.setCounters(metricsToMap(offset));
              return r;
            });
  }
}
