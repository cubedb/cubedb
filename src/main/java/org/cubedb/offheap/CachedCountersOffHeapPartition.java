package org.cubedb.offheap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.cubedb.core.Constants;
import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class CachedCountersOffHeapPartition extends OffHeapPartition {

	Map<String, Map<String, Map<String, MutableLong>>> counters;
	private static final Logger log = LoggerFactory.getLogger(CachedCountersOffHeapPartition.class);
	
	public CachedCountersOffHeapPartition() {
		this.counters = new HashMap<String, Map<String, Map<String, MutableLong>>>();
		// initializeCounters();
	}

	@Override
	protected void addMetric(String metricName) {
		super.addMetric(metricName);
		initializeCounters();
	}

	protected synchronized void initializeCounters() {
		SearchResult r = super.get(new ArrayList<Filter>());
		this.counters = new HashMap<String, Map<String, Map<String, MutableLong>>>();
		for (Entry<SearchResultRow, Long> e : r.getResults().entrySet()) {
			String fieldName = e.getKey().getFieldName();
			Objects.requireNonNull(fieldName);
			String fieldValue = e.getKey().getFieldValue();
			Objects.requireNonNull(fieldValue);
			String metric = e.getKey().getMetricName();
			Objects.requireNonNull(metric);
			long value = e.getValue().longValue();
			this.counters.computeIfAbsent(fieldName, k -> new HashMap<String, Map<String, MutableLong>>())
					.computeIfAbsent(fieldValue, k -> new HashMap<String, MutableLong>())
					.computeIfAbsent(metric, k -> new MutableLong()).increment(value);

		}
	}

	/*
	 * @Override protected void initializeMap(Set<String> metricNames) {
	 * super.initializeMap(metricNames); if(this.metricLookup.size()==0){ // no
	 * metrics are defined yet, we can create them for(String metric:
	 * metricNames){ this.addMetric(metric); } } log.info(
	 * "Re-Initializing counters"); long t0 = System.currentTimeMillis();
	 * this.initializeCounters(); log.debug(
	 * "Done initializing counters. Took {}ms", System.currentTimeMillis() -
	 * t0); }
	 */

	@Override
	public synchronized void insert(DataRow row) {
		super.insert(row);
		if(this.counters==null)
		{
			this.initializeCounters();
		}
		
		final Set<Entry<String, Long>> metricsEntrySet = row.getCounters().entrySet();
		for (Entry<String, String> fieldEntry : row.getFields().entrySet()) {
			final String fieldName = fieldEntry.getKey()!=null?fieldEntry.getKey():Constants.NULL_VALUE;
			final String fieldValue = fieldEntry.getValue()!=null?fieldEntry.getValue():Constants.NULL_VALUE;
			final Map<String, MutableLong> metrics = counters
					.computeIfAbsent(fieldName, k -> new HashMap<String, Map<String, MutableLong>>())
					.computeIfAbsent(fieldValue, k -> new HashMap<String, MutableLong>());
			for (Entry<String, Long> m : metricsEntrySet) {
				metrics.computeIfAbsent(m.getKey(), k -> new MutableLong()).increment(m.getValue().longValue());
			}
		}
	}

	@Override
	public SearchResult get(List<Filter> filters) {
		if (filters.size() == 0) {
			if(counters==null)
			{
				this.initializeCounters();
			}
			//log.info("Returning cached results");
			final Map<String, MutableLong> totalCounts = new HashMap<String, MutableLong>();
			final Map<SearchResultRow, Long> results = new HashMap<SearchResultRow, Long>();
			for (final Entry<String, Map<String, Map<String, MutableLong>>> e : counters.entrySet())
				for (final Entry<String, Map<String, MutableLong>> ee : e.getValue().entrySet())
					for (final Entry<String, MutableLong> m : ee.getValue().entrySet()) {
						final long val = m.getValue().get();
						results.put(new SearchResultRow(e.getKey(), ee.getKey(), m.getKey()), val);
						totalCounts.computeIfAbsent(m.getKey(), k -> new MutableLong()).increment(val);
					}

			final Map<String, Long> c = totalCounts.entrySet().stream()
					.collect(Collectors.toMap(Entry::getKey, v -> v.getValue().get()));
			final SearchResult result = new SearchResult(results, c);
			return result;
		}
		return super.get(filters);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		super.read(kryo, input);
		//initializeCounters();
	}
}
