package org.cubedb.offheap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cubedb.api.KeyMap;
import org.cubedb.core.Column;
import org.cubedb.core.ColumnDoesNotExistException;
import org.cubedb.core.Constants;
import org.cubedb.core.Metric;
import org.cubedb.core.Partition;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.core.lookups.HashMapLookup;
import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.tiny.TinyColumn;
import org.cubedb.core.tiny.TinyMetric;
import org.cubedb.core.tiny.TinyUtils;
import org.cubedb.offheap.matchers.IdMatcher;
import org.cubedb.utils.CubeUtils;
import org.cubedb.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class OffHeapPartition implements Partition {

	protected Map<String, Lookup> lookups;
	protected Lookup fieldLookup;
	protected Lookup metricLookup;
	protected Map<String, Column> columns;
	protected Map<String, Metric> metrics;
	protected int size;
	protected KeyMap map;
	private static final Logger log = LoggerFactory.getLogger(OffHeapPartition.class);
	protected long lastInsertTs;
	protected long lastAppendTs;
	protected long startupTs;
	protected long lastSaveTs;

	public OffHeapPartition() {
		// log.debug("Initializing Partition");
		this.lookups = new HashMap<String, Lookup>(5);
		this.fieldLookup = new HashMapLookup(false);
		this.columns = new HashMap<String, Column>(5);
		this.metrics = new HashMap<String, Metric>(1);
		this.metricLookup = new HashMapLookup(false);
		this.createMap(1);

	}

	protected void addColumn(String columnName) {
		Column col = new TinyColumn(size);
		this.columns.putIfAbsent(columnName, col);
	}

	protected void addMetric(String metricName) {
		Metric m = new TinyMetric(size);
		this.metrics.putIfAbsent(metricName, m);
		this.metricLookup.getValue(metricName);
	}

	protected void createMap(int fieldsLength) {
		// this.map = new MapDBKeyMap(size, fieldsLength);
		this.map = new DummyKeyMap(size, fieldsLength);
		this.map = new BOHKeyMap(size, fieldsLength);
	}

	protected void initializeMap() {
		// log.info("Re-Initializing map");
		long t0 = System.currentTimeMillis();
		final Column[] fields = new Column[fieldLookup.getKeys().length];
		Metric[] metrics = new Metric[this.metrics.size()];
		// int fieldValues[] = new int[fields.length];
		for (int i = 0; i < metrics.length; i++) {
			metrics[i] = this.metrics.get(this.metricLookup.getKey(i));
		}

		for (int i = 0; i < fields.length; i++) {
			String fieldKey = fieldLookup.getKey(i);
			fields[i] = columns.get(fieldKey);
		}

		this.createMap(fields.length);

		byte[] b = new byte[fields.length * Short.BYTES];
		ByteBuffer bb = ByteBuffer.wrap(b);
		int j = 0;
		long[] metricValues = new long[metrics.length];
		for (int i = 0; i < size; i++) {
			bb.clear();
			for (int m = 0; m < metrics.length; m++) {
				metricValues[m] += metrics[m].get(i);
			}
			for (j = 0; j < fields.length; j++) {
				Column c = fields[j];
				short val = (short) c.get(i);
				bb.putShort(val);

			}

			this.map.put(b, i);
		}
		// log.debug("Done initializing map. Took {}ms",
		// System.currentTimeMillis() - t0);
		// System.gc();
	}

	// TODO: refactor to accept int[], long[]
	protected void insertFields(short[] fields, Map<String, Long> metrics) {
		ByteBuffer buf = ByteBuffer.allocate(fields.length * Integer.BYTES);
		buf.clear();
		for (short f : CubeUtils.cutZeroSuffix(fields))
			buf.putShort(f);
		byte[] bytes = buf.array();
		Integer index = this.map.get(bytes);

		// 1. Check if there are any new metrics
		for (String metricName : metrics.keySet()) {
			if (!this.metrics.containsKey(metricName)) {
				// log.info("New metric {} found", metricName);
				if (size != 0)
					throw new RuntimeException("Adding new metrics on fly is not implemented yet");
				this.addMetric(metricName);
			}
		}

		// 2. Check if this combination of fields has ever existed.
		// If never existed, create one.
		if (index == null) {
			// log.debug("Inserting new combination of dimensions into
			// partition");
			index = new Integer(size);
			this.map.put(bytes, index);
			for (int i = 0; i < fields.length; i++) {
				String fieldName = this.fieldLookup.getKey(i);
				// log.debug("Writing {}={} to buffers", fieldName, fields[i]);
				Column col = this.columns.get(fieldName);
				if (col.isTiny() && col.getNumRecords() > Constants.INITIAL_PARTITION_SIZE) {
					// log.debug("There are {} records, converting TinyColumn {}
					// to OffHeap", col.getNumRecords(), fieldName);
					col = TinyUtils.tinyColumnToOffHeap((TinyColumn) col);
					this.columns.put(fieldName, col);
				}
				col.append(fields[i]);
			}
			for (Entry<String, Metric> e : this.metrics.entrySet()) {
				Metric m = e.getValue();
				String metricName = e.getKey();
				if (m.isTiny() && m.getNumRecords() > Constants.INITIAL_PARTITION_SIZE) {
					// log.debug("Converting TinyMetric {} to OffHeap",
					// metricName);
					m = TinyUtils.tinyMetricToOffHeap((TinyMetric) m);
					this.metrics.put(metricName, m);
				}
				m.append(0l);
			}
			this.lastAppendTs = System.currentTimeMillis();
			size++;
		}

		// 3. Increment metrics by one
		for (Entry<String, Metric> e : this.metrics.entrySet()) {
			Long c = metrics.get(e.getKey()).longValue();
			if (c != null) {
				e.getValue().incrementBy(index.intValue(), c.longValue());
			}

		}

	}

	protected void addNewFields(DataRow row) {
		Set<String> newFields = new HashSet<String>(row.getFields().keySet());
		for (String f : this.fieldLookup.getKeys())
			if (row.getFields().containsKey(f))
				newFields.remove(f);

		// log.info("The following new fields where detected: {}",
		// newFields.toString());
		if (newFields.size() > 0) {
			for (String f : newFields) {
				final int newColumnIndex = this.fieldLookup.getValue(f);
				this.lookups.put(f, new HashMapLookup());
				// log.debug("Index for {} is {}", f, newColumnIndex);
				this.columns.put(f, new TinyColumn(this.size));
			}
			// initializeMap(row.getCounters().keySet());
			// log.info("Re-inserting {}", row.getFields());
			this._insert(row);
		}
	}

	protected void _insert(DataRow row) {
		short[] fields = new short[this.fieldLookup.size()];
		int i = 0;

		int insertedFieldCount = 0;
		for (String fieldName : this.fieldLookup.getKeys()) {
			String value = row.getFields().get(fieldName);
			int valueIndex = 0;
			if (value != null || row.getFields().containsKey(fieldName)) {
				insertedFieldCount++;
				valueIndex = this.lookups.get(fieldName).getValue(value != null ? value : Constants.NULL_VALUE);
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
			this.addNewFields(row);
		} else {
			this.insertFields(fields, row.getCounters());
		}

	}

	@Override
	public synchronized void insert(DataRow row) {
		this._insert(row);
		this.lastInsertTs = System.currentTimeMillis();
	}

	public void insertData(List<DataRow> data) {
		log.info("Inserting {} rows", data.size());
		long t0 = System.nanoTime();
		data.forEach(this::insert);
		long t1 = System.nanoTime() - t0;
		long rowsPerSecond = 1000000000l * data.size() / t1;
		log.info("Took {}ms to insert {} rows, {}mks/row", t1 / 1000 / 1000, data.size(), t1 / data.size() / 1000);
		log.info("That is {} rows/sec", rowsPerSecond);
	}

	protected Map<String, IdMatcher> transformFiltersToMatchers(List<Filter> filters)
			throws ColumnDoesNotExistException {
		// log.debug("List of filters: {}", filters);
		Map<String, IdMatcher> out = new HashMap<String, IdMatcher>();
		Map<String, Set<String>> filtersByColumn = new HashMap<String, Set<String>>();
		for (String columnName : this.columns.keySet()) {
			filtersByColumn.put(columnName, new HashSet<String>());
		}
		for (Filter f : filters) {
			if (!this.columns.containsKey(f.getField())) {
				// the column we are filtering for does not exist
				// so, if we are looking for null value, then we are fine.
				if (f.getValues().length == 1
						&& (f.getValues()[0].equals(Constants.NULL_VALUE) || f.getValues()[0] == null)) {
					log.info("There is only one null value");
					continue;
				} else {
					final String msg = String.format("Column %s does not exist in this partition", f.getField());
					throw new ColumnDoesNotExistException(msg);
				}
				// otherwise we will never find anything over here

			}
			for (String v : f.getValues())
				filtersByColumn.get(f.getField()).add(v);
		}
		for (Entry<String, Set<String>> e : filtersByColumn.entrySet()) {
			int[] idList = e.getValue().stream().mapToInt((val) -> this.lookups.get(e.getKey()).getValue(val))
					.toArray();
			if (idList.length > 0) {
				// log.debug("Transforming values of {} to {} in {}",
				// e.getValue(), idList, e.getKey());
				out.put(e.getKey(), new IdMatcher(idList));
			}
		}
		// log.debug("Resulting id matchers: {}", out);
		return out;

	}

	protected long[][][] initSideCounters() {
		// log.debug("Metrics look like this: {}",
		// (Object)this.metricLookup.getKeys());
		final long[][][] out = new long[this.fieldLookup.size()][][];
		for (int i = 0; i < this.fieldLookup.size(); i++) {
			String fieldName = this.fieldLookup.getKey(i);
			Lookup side = this.lookups.get(fieldName);
			long[][] sideCounters = new long[side.size()][this.metricLookup.size()];
			for (int s = 0; s < sideCounters.length; s++)
				for (int m = 0; m < this.metricLookup.size(); m++)
					sideCounters[s][m] = 0l;
			out[i] = sideCounters;
		}
		// log.debug("Initial counters look like this {}", (Object)out);
		return out;
	}

	protected Column[] getColumnsAsArray() {
		final Column[] columns = new Column[this.columns.size()];
		for (Entry<String, Column> e : this.columns.entrySet()) {
			columns[this.fieldLookup.getValue(e.getKey())] = e.getValue();
		}
		return columns;
	}

	protected SearchResult convertToResult(long[][][] sideCounters, long[] totalCounters) {
		final Map<SearchResultRow, Long> result = new HashMap<SearchResultRow, Long>();
		for (int i = 0; i < sideCounters.length; i++) {
			String sideName = this.fieldLookup.getKey(i);
			final Lookup sideLookup = this.lookups.get(sideName);
			for (int j = 0; j < sideCounters[i].length; j++) {
				String sideValue = sideLookup.getKey(j);
				for (int m = 0; m < sideCounters[i][j].length; m++) {
					String metricName = this.metricLookup.getKey(m);
					SearchResultRow r = new SearchResultRow();
					r.setFieldName(sideName);
					r.setFieldValue(sideValue);
					r.setMetricName(metricName);
					result.put(r, sideCounters[i][j][m]);
				}
			}

		}
		Map<String, Long> totalCounts = new HashMap<String, Long>(totalCounters.length);
		for (int i = 0; i < totalCounters.length; i++)
			totalCounts.put(this.metricLookup.getKey(i), totalCounters[i]);
		// log.debug("Converted {} to {}", sideCounters, result);
		return new SearchResult(result, totalCounts);
	}

	protected SearchResult getEmptySearchResult() {
		Map<String, Long> totalCounts = new HashMap<String, Long>();
		for (String metricName : this.metrics.keySet()) {
			totalCounts.put(metricName, 0l);
		}
		SearchResult r = new SearchResult(new HashMap<SearchResultRow, Long>(), totalCounts);
		return r;
	}

	@Override
	public SearchResult get(List<Filter> filters) {
		// log.debug("Starting search");
		long t0 = System.nanoTime();
		int curSize = size;
		// creating an empty result set, with id's
		final String[] metricNames = this.metricLookup.getKeys();
		final long[] totalCounters = new long[metricNames.length];

		// field names -> (field id -> (metric name -> counter))
		final long[][][] sideCounters = initSideCounters();
		final Column[] columns = getColumnsAsArray();

		int matchCount = 0;
		final long t2, t3;
		// creating a map of matchers based on filter
		final boolean[] columnMatches = new boolean[this.fieldLookup.size()];
		final long metricValues[] = new long[metricNames.length];
		final int columnValues[] = new int[this.fieldLookup.size()];
		final IdMatcher[] matchersArray = new IdMatcher[this.fieldLookup.size()];
		final Metric[] metricsArray = new Metric[this.metricLookup.size()];
		try {
			final Map<String, IdMatcher> matchers = transformFiltersToMatchers(filters);

			for (Entry<String, IdMatcher> e : matchers.entrySet()) {
				int field_id = this.fieldLookup.getValue(e.getKey());
				matchersArray[field_id] = e.getValue();
			}

			for (Entry<String, Metric> e : metrics.entrySet()) {
				int field_id = this.metricLookup.getValue(e.getKey());
				metricsArray[field_id] = e.getValue();
			}

			// matching itself
			t2 = System.nanoTime();
			for (int i = 0; i < curSize; i++) {
				// boolean rowMatches = true;

				for (int matcherId = 0; matcherId < matchersArray.length; matcherId++) {
					IdMatcher matcher = matchersArray[matcherId];
					columnMatches[matcherId] = true;
					final int valueId = columns[matcherId].get(i);
					columnValues[matcherId] = valueId;
					if (matcher != null) {
						columnMatches[matcherId] = matcher.match(valueId);
					}
				}
				if (atLeastOneMatch(columnMatches)) {
					// We have a match!
					// First, we retrieve the counters
					for (int mIndex = 0; mIndex < metricNames.length; mIndex++) {
						final long c = metricsArray[mIndex].get(i);
						metricValues[mIndex] = c;
					}

					for (int side = 0; side < this.fieldLookup.size(); side++) {
						final int columnId = columnValues[side];
						MatchType matchType = checkColumnMatch(columnMatches, side);
						if (matchType != MatchType.NO_MATCH)
							// this row matches other filters
							for (int mIndex = 0; mIndex < metricNames.length; mIndex++) {
								sideCounters[side][columnId][mIndex] += metricValues[mIndex];

								if (side == 0 && matchType == MatchType.ALL_COLUMNS_MATCH) {
									// in fact, the row matches all filters
									// we are going to increase the total
									// counters.
									totalCounters[mIndex] += metricValues[mIndex];
								}
							}
					}
				}
			}
			t3 = System.nanoTime();

		} catch (ColumnDoesNotExistException e) {
			log.warn(e.getMessage());
			return getEmptySearchResult();
		}
		final SearchResult result = convertToResult(sideCounters, totalCounters);
		final long t1 = System.nanoTime();
		// log.debug("Got {} matches for the query in {}ms among {} rows",
		// matchCount, (t1 - t0) / 1000000.0, curSize);
		if (curSize > 0 && (t3 - t2) > 0) {
			int rowsPerSecond = (int) (1000000000l * curSize / (t3 - t2));
			// log.debug("Bruteforce search itself took {} ms", (t3 - t2) /
			// 1000000.0);
			// log.debug("Bruteforce search is {} rows/second", rowsPerSecond);
		}
		return result;

	}

	private MatchType checkColumnMatch(boolean[] matches, int side) {
		MatchType currentMatch = MatchType.ALL_COLUMNS_MATCH;

		for (int i = 0; i < matches.length; i++) {
			if (!matches[i]) {
				if (i == side)
					currentMatch = MatchType.OTHER_COLUMNS_MATCH;
				else {
					return MatchType.NO_MATCH;
				}
			}
		}
		return currentMatch;
	}

	private boolean atLeastOneMatch(boolean[] matches) {
		//boolean atLeastOneMatch = false;
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
		long columnSize = this.columns.values().stream().mapToLong(Column::size).sum();
		long metricSize = this.metrics.values().stream().mapToLong(Metric::size).sum();
		int columnBlocks = this.columns.values().stream().mapToInt(Column::getNumBuffers).sum();
		int metricBLocks = this.metrics.values().stream().mapToInt(Metric::getNumBuffers).sum();
		long lookupSize = (long) this.map.size() * this.columns.size() * Short.BYTES;
		Map<String, Object> stats = new HashMap<String, Object>();
		stats.put(Constants.STATS_COLUMN_SIZE, columnSize);
		stats.put(Constants.STATS_METRIC_SIZE, metricSize);
		stats.put(Constants.STATS_COLUMN_BLOCKS, columnBlocks);
		stats.put(Constants.STATS_METRIC_BLOCKS, metricBLocks);
		stats.put(Constants.STATS_LOOKUP_SIZE, lookupSize);
		stats.put(Constants.STATS_LAST_INSERT, this.lastInsertTs);
		stats.put(Constants.STATS_LAST_RECORD_APPEND, this.lastAppendTs);
		stats.put(Constants.STATS_NUM_RECORDS, this.size);
		stats.put(Constants.STATS_NUM_COLUMNS, this.columns.size());
		stats.put(Constants.STATS_NUM_LARGE_BLOCKS,
				this.metrics.values().stream().mapToInt(e -> e.isTiny() ? 0 : 1).sum()
						+ this.columns.values().stream().mapToInt(e -> e.isTiny() ? 0 : 1).sum());
		// stats.put(Constants.STATS_LAST_SAVE, this.lastInsertTs);
		this.lookups.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().size()));
		return stats;
	}

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(this.size);
		// add fieldlookup
		kryo.writeClassAndObject(output, this.fieldLookup);
		// add dimension lookups
		kryo.writeClassAndObject(output, this.lookups);
		// add metriclookup
		kryo.writeClassAndObject(output, this.metricLookup);
		// add columns
		kryo.writeClassAndObject(output, this.columns);
		// add metrics
		kryo.writeClassAndObject(output, this.metrics);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(Kryo kryo, Input input) {
		this.size = input.readInt();
		this.fieldLookup = (Lookup) kryo.readClassAndObject(input);
		this.lookups = (Map<String, Lookup>) kryo.readClassAndObject(input);
		this.metricLookup = (Lookup) kryo.readClassAndObject(input);
		this.columns = (Map<String, Column>) kryo.readClassAndObject(input);
		this.metrics = (Map<String, Metric>) kryo.readClassAndObject(input);
		this.initializeMap();
	}

	protected Map<String, String> bytesToMap(byte[] in) {
		Map<String, String> out = new HashMap<String, String>();
		ByteBuffer b = ByteBuffer.wrap(in);
		for (int i = 0; i < this.fieldLookup.size(); i++) {
			String fieldName = this.fieldLookup.getKey(i);
			String fieldValue;
			if (b.remaining() > 0) {
				int fieldId = b.getShort();
				fieldValue = this.lookups.get(fieldName).getKey(fieldId);
				fieldValue = fieldValue.equals("null") ? null : fieldValue;
			} else
				fieldValue = null;
			out.put(fieldName, fieldValue);

		}
		return out;
	}

	protected Map<String, Long> metricsToMap(int offset) {
		Map<String, Long> out = new HashMap<String, Long>();
		for (int i = 0; i < this.metricLookup.size(); i++) {
			String metricName = this.metricLookup.getKey(i);
			Long metricValue = this.metrics.get(metricName).get(offset);
			out.put(metricName, metricValue);
		}
		return out;
	}

	@Override
	public Stream<DataRow> asDataRowStream() {
		return this.map.entrySet().map(e -> {
			DataRow r = new DataRow();
			r.setFields(this.bytesToMap(e.getKey()));
			int offset = e.getValue();
			r.setCounters(metricsToMap(offset));
			return r;
		});

	}

}
