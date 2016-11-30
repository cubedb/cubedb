package org.cubedb.offheap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class CachedCountersOffHeapPartition extends OffHeapPartition {

	Map<String, Map<String, Map<String, MutableLong>>> counters;
	Map<String, MutableLong> totalCounters;
	private static final Logger log = LoggerFactory.getLogger(CachedCountersOffHeapPartition.class);

	public CachedCountersOffHeapPartition() {
		this.counters = null; // new HashMap<String, Map<String, Map<String,
								// MutableLong>>>();
		// initializeCounters();
	}

	@Override
	protected void addMetric(String metricName) {
		super.addMetric(metricName);
		initializeCounters();
	}

	protected synchronized void initializeCounters() {
		log.info("Initializing counters");
		SearchResult r = super.get(new ArrayList<Filter>());
		this.counters = new HashMap<String, Map<String, Map<String, MutableLong>>>();
		this.totalCounters = new HashMap<String, MutableLong>();
		for (Entry<GroupedSearchResultRow, Long> e : r.getResults().entrySet()) {
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
			this.totalCounters.computeIfAbsent(metric, k -> new MutableLong()).increment(value);
		}

	}

	@Override
	public synchronized void insert(DataRow row) {
		super.insert(row);
		if (this.counters == null) {
			this.initializeCounters();
		}

		this.updateCounters(row);
	}

	protected Map<String, MutableLong> copyCountersFromTotal(Set<Entry<String, Long>> metricsEntrySet)
	{
		Map<String, MutableLong> counters = new HashMap<String, MutableLong>();
		for (Entry<String, Long> m : metricsEntrySet) {
			counters.put(m.getKey(),
					new MutableLong(this.totalCounters.computeIfAbsent(m.getKey(), (kk) -> new MutableLong()).get())
					);
		}
		return counters;
	}

	protected void updateCounters(DataRow row)
	{
		final Set<Entry<String, Long>> metricsEntrySet = row.getCounters().entrySet();
		Set<String> unUsedFields = new HashSet<String>();
		unUsedFields.addAll(this.counters.keySet());
		for (Entry<String, String> fieldEntry : row.getFields().entrySet()) {
			final String fieldName = fieldEntry.getKey() != null ? fieldEntry.getKey() : Constants.NULL_VALUE;
			final String fieldValue = fieldEntry.getValue() != null ? fieldEntry.getValue() : Constants.NULL_VALUE;
			unUsedFields.remove(fieldName);
			// Now we increment all the metrics for this fieldName and fieldValue
			// First, we retrieve the metrics for this fieldName->fieldValue
			final Map<String, MutableLong> metrics = this.counters
					.computeIfAbsent(fieldName, k -> {
						// if there is a new field, we pretend it existed before, but only had nulls
						Map<String, Map<String, MutableLong>> side = new HashMap<String, Map<String, MutableLong>>();
						Map<String, MutableLong> counters = copyCountersFromTotal(metricsEntrySet);
						//log.debug("Adding {}->{} with value {}", fieldName, Constants.NULL_VALUE, counters);
						side.put(Constants.NULL_VALUE, counters);
						return side;

						})
					.computeIfAbsent(fieldValue, k -> new HashMap<String, MutableLong>());
			// then we increment those metrics one by one. We create a new metric.
			for (Entry<String, Long> m : metricsEntrySet) {
				String metricName = m.getKey();
				final long val = m.getValue().longValue();
				//log.debug("Incrementing {}->{}->{} by {}", fieldName, fieldValue, metricName, val);
				metrics.computeIfAbsent(metricName, k -> new MutableLong()).increment(val);
				//log.debug("Now {}->{}->{} is {}", fieldName, fieldValue, metricName, metrics.get(metricName).get());
			}
			//this.counters.get(fieldName).put(fieldValue, metrics);
			//log.debug("Counters now look like this: {}", this.counters);
		}
		for(String fieldName: unUsedFields)
		{
			for(Entry<String, Long> m : metricsEntrySet)
			{
				this.counters.get(fieldName).get(Constants.NULL_VALUE).get(m.getKey()).increment(m.getValue().longValue());
			}
		}

		for (Entry<String, Long> m : metricsEntrySet) {
			String metricName = m.getKey();
			long val = m.getValue().longValue();
			this.totalCounters.computeIfAbsent(metricName, (kk) -> new MutableLong()).increment(val);
		}
		//log.debug("Counters now look like this: {}", this.counters);
		//log.debug("totalCounters now look like this: {}", this.totalCounters);
	}

	@Override
	public SearchResult get(List<Filter> filters) {
		if (filters.size() == 0) {
			if (counters == null) {
				this.initializeCounters();
			}
			// log.info("Returning cached results");
			final Map<String, MutableLong> totalCounts = new HashMap<String, MutableLong>();
			final Map<GroupedSearchResultRow, Long> results = new HashMap<GroupedSearchResultRow, Long>();
			boolean firstSide = true;
			for (final Entry<String, Map<String, Map<String, MutableLong>>> e : counters.entrySet()) {
				for (final Entry<String, Map<String, MutableLong>> ee : e.getValue().entrySet()) {
					//log.debug("ee is {}", ee.getKey());
					for (final Entry<String, MutableLong> m : ee.getValue().entrySet()) {
						//log.debug("m is {}", m.getKey());
						final long val = m.getValue().get();
						results.put(new GroupedSearchResultRow(e.getKey(), ee.getKey(), m.getKey()), val);
						if (firstSide){
							// we calculate totals only once
							totalCounts.computeIfAbsent(m.getKey(), k -> new MutableLong()).increment(val);
						}
					}

				}
				firstSide = false;
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
		// initializeCounters();
	}
}
