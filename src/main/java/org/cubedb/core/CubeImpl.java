package org.cubedb.core;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.Pair;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.offheap.OffHeapPartition;
import org.cubedb.utils.CubeUtils;
import org.cubedb.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.owlike.genson.Genson;

public class CubeImpl implements Cube {
	public static final Logger log = LoggerFactory.getLogger(CubeImpl.class);
	Map<String, Partition> partitions;
	String partitionColumn;

	public CubeImpl(String partitionColumn) {
		partitions = new ConcurrentHashMap<String, Partition>();
		this.partitionColumn = partitionColumn;
	}

	private Partition createNewPartition(String partitionName) {
		Partition p = new OffHeapPartition();
		return p;
	}

	protected void insert(Collection<String> newPartitions, Map<String, List<DataRow>> groupedData) {
		for (String p : newPartitions) {
			Partition partition = partitions.computeIfAbsent(p, this::createNewPartition);
			for (DataRow d : groupedData.get(p)) {
				partition.insert(d);

			}
		}
	}

	@Override
	public int optimize() {
		return partitions.values().stream().mapToInt(p -> p.optimize() ? 1 : 0).sum();
	}

	protected class Insertor implements Runnable {
		final private List<String> partitions;
		final private Map<String, List<DataRow>> groupedData;

		public Insertor(List<String> partitions, Map<String, List<DataRow>> groupedData) {
			this.partitions = partitions;
			this.groupedData = groupedData;
		}

		@Override
		public void run() {
			insert(partitions, groupedData);
		}

	}

	public void insertSequential(Map<String, List<DataRow>> groupedData) {
		insert(groupedData.keySet(), groupedData);
	}

	public void insertParallel(Map<String, List<DataRow>> groupedData) {
		int numThreads = Runtime.getRuntime().availableProcessors();
		// numThreads = 2;
		List<List<String>> partitionSequences = CubeUtils.partitionList(new ArrayList<String>(groupedData.keySet()));
		log.debug("Will be using {} threads, with {} processors", partitionSequences.size(), numThreads);
		Insertor[] insertors = new Insertor[partitionSequences.size()];
		Thread[] threads = new Thread[insertors.length];
		for (int i = 0; i < insertors.length; i++) {
			insertors[i] = new Insertor(partitionSequences.get(i), groupedData);
			threads[i] = new Thread(insertors[i]);
			threads[i].start();
		}
		for (int i = 0; i < insertors.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				log.error("Interrupted");
			}
		}
	}

	public void insert(List<DataRow> data) {
		Map<String, List<DataRow>> groupedData = data.stream().collect(Collectors.groupingBy(DataRow::getPartition));
		// insertSequential(groupedData);
		// log.debug("The following partitions detected: {}",
		// groupedData.keySet());
		log.debug("Size of items: {}, number of groups: {}", data.size(), groupedData.size());
		if (data.size() < 1500 || groupedData.size() <= 3) {
			log.debug("Using sequential insert");
			insertSequential(groupedData);
		} else {
			log.debug("Using parallel insert");
			insertParallel(groupedData);
		}
	}

	protected Map<GroupedSearchResultRow, MutableLong> get(List<Pair<String, Partition>> partitions,
			List<Filter> filters, String fromPartition, String toPartition, String groupBy) {
		boolean isGroupLookup = groupBy != null;
		String groupFieldName = isGroupLookup ? groupBy : SearchResult.FAKE_GROUP_FIELD_NAME;

		Map<GroupedSearchResultRow, MutableLong> out = new HashMap<GroupedSearchResultRow, MutableLong>(1000, 0.5f);
		for (Pair<String, Partition> e : partitions) {
			String partitionValue = e.getKey();
			Partition partition = e.getValue();
			SearchResult searchResult = partition.get(filters, groupBy);

			boolean isPartitionMatch = partitionValue.compareTo(fromPartition) >= 0
					&& partitionValue.compareTo(toPartition) <= 0;
			if (isPartitionMatch) {
				for (final Entry<GroupedSearchResultRow, Long> sr : searchResult.getResults().entrySet()) {
					GroupedSearchResultRow row = sr.getKey();
					Long rowValue = sr.getValue();
					out.computeIfAbsent(row, k -> new MutableLong()).increment(rowValue);
				}
			}

			for (Entry<String, Map<String, Long>> tc : searchResult.getTotalCounts().entrySet()) {
				String metricName = tc.getKey();
				Map<String, Long> groupFieldValueToCount = tc.getValue();
				for (Entry<String, Long> gc : groupFieldValueToCount.entrySet()) {
					Long metricValue = gc.getValue();
					String groupFieldValue = gc.getKey();
					GroupedSearchResultRow row = new GroupedSearchResultRow(groupFieldName, groupFieldValue,
							partitionColumn, partitionValue, metricName);
					out.put(row, new MutableLong(metricValue));
				}
			}
		}
		return out;
	}

	class Searcher implements Runnable {
		final private List<Pair<String, Partition>> partitions;
		private Map<GroupedSearchResultRow, MutableLong> result;
		final private List<Filter> filters;
		final private String fromPartition, toPartition, groupBy;

		public Searcher(List<Pair<String, Partition>> partitions, List<Filter> filters, String fromPartition,
				String toPartition, String groupBy) {
			this.partitions = partitions;
			this.filters = filters;
			this.fromPartition = fromPartition;
			this.toPartition = toPartition;
			this.groupBy = groupBy;
		}

		@Override
		public void run() {
			result = get(partitions, filters, fromPartition, toPartition, groupBy);
		}

		public Map<GroupedSearchResultRow, MutableLong> getResult() {
			return result;
		};

	}

	@Override
	public Map<GroupedSearchResultRow, Long> get(final String fromPartition, final String toPartition,
			List<Filter> filters, String groupBy) {
		long t0 = System.currentTimeMillis();
		List<Pair<String, Partition>> namePartitionPair = partitions.entrySet().stream()
				.filter((e) -> e.getKey().compareTo(fromPartition) >= 0 && e.getKey().compareTo(toPartition) <= 0)
				.map(e -> new Pair<String, Partition>(e.getKey(), e.getValue())).collect(Collectors.toList());
		List<Filter> realFilters = filters.stream().filter((f) -> !f.getField().equals(partitionColumn))
				.collect(Collectors.toList());
		List<Filter> partitionFilters = filters.stream().filter((f) -> f.getField().equals(partitionColumn))
				.collect(Collectors.toList());

		final String fromPartitionFilter = partitionFilters.stream()
				.map(f -> Arrays.stream(f.getValues()).min(String::compareTo).get()).min(String::compareTo)
				.orElse(fromPartition);
		final String toPartitionFilter = partitionFilters.stream()
				.map(f -> Arrays.stream(f.getValues()).max(String::compareTo).get()).max(String::compareTo)
				.orElse(toPartition);
		Collections.shuffle(namePartitionPair);
		final List<List<Pair<String, Partition>>> partitionSlices = CubeUtils.partitionList(namePartitionPair);

		Searcher[] searchers = new Searcher[partitionSlices.size()];
		Thread[] threads = new Thread[partitionSlices.size()];
		for (int i = 0; i < searchers.length; i++) {
			searchers[i] = new Searcher(partitionSlices.get(i), realFilters, fromPartitionFilter, toPartitionFilter,
					groupBy);
			threads[i] = new Thread(searchers[i]);
			threads[i].start();
		}

		try {
			for (Thread thread : threads) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long tPreReduce = System.currentTimeMillis();
		log.debug("Search pre-reduce took {}ms", tPreReduce - t0);

		Map<GroupedSearchResultRow, MutableLong> result = new HashMap<GroupedSearchResultRow, MutableLong>();
		for (Searcher searcher : searchers) {
			for (Entry<GroupedSearchResultRow, MutableLong> e : searcher.getResult().entrySet()) {
				GroupedSearchResultRow row = e.getKey();
				MutableLong c = result.computeIfAbsent(row, newRow -> new MutableLong());
				MutableLong rowValue = e.getValue();
				c.increment(rowValue.get());
			}
		}

		long t1 = System.currentTimeMillis();
		log.debug("Reduce took {}ms", t1 - tPreReduce);

		return result.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
	}

	@Override
	public void deletePartition(String partitionName) {
		partitions.remove(partitionName);

	}

	@Override
	public Map<GroupedSearchResultRow, Long> get(int lastRange, List<Filter> filters, String groupBy) {
		List<String> partitionKeys = partitions.keySet().stream().sorted((e, ot) -> ot.compareTo(e)).limit(lastRange)
				.collect(Collectors.toList());
		if (partitionKeys.size() == 0) {
			return new HashMap<GroupedSearchResultRow, Long>();
		}
		final String toPartition = partitionKeys.get(0);
		final String fromPartition = partitionKeys.get(partitionKeys.size() - 1);

		return get(fromPartition, toPartition, filters, groupBy);
	}

	@Override
	public TreeSet<String> getPartitions(final String from, final String to) {
		return partitions.keySet().stream().filter(p -> from == null || from.compareTo(p) <= 0)
				.filter(p -> from == null || to.compareTo(p) >= 0).sorted()
				.collect(Collectors.toCollection(() -> new TreeSet<String>()));
	}

	@Override
	public void save(String saveFileName) throws IOException {
		Kryo kryo = CubeUtils.getKryoWithRegistrations();
		OutputStream stream;
		if (saveFileName.endsWith(".gz"))
			stream = new GZIPOutputStream(new FileOutputStream(saveFileName));
		else
			stream = new SnappyOutputStream(new FileOutputStream(saveFileName));
		Output output = new Output(stream);
		kryo.writeClassAndObject(output, partitions);
		output.close();
	}

	@Override
	public void load(String saveFileName) throws IOException {
		Kryo kryo = CubeUtils.getKryoWithRegistrations();
		InputStream stream;
		if (saveFileName.endsWith(".gz"))
			stream = new GZIPInputStream(new FileInputStream(saveFileName));
		else if (saveFileName.endsWith(".snappy"))
			stream = new SnappyInputStream(new FileInputStream(saveFileName));
		else
			throw new IOException("Cannot recognize the file extension for file " + saveFileName);
		Input input = new Input(stream);
		partitions = (Map<String, Partition>) kryo.readClassAndObject(input);
		input.close();
		System.gc();
	}

	@Override
	public Map<String, Object> getStats() {
		Map<String, Map<String, Object>> partitionStats = partitions.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getStats()));
		Map<String, Object> out = new HashMap<String, Object>();
		// out.put("partitionStats", partitionStats);
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
		out.put(Constants.STATS_NUM_LARGE_BLOCKS, partitionStats.values().stream()
				.mapToInt(e -> (Integer) e.get(Constants.STATS_NUM_LARGE_BLOCKS)).sum());
		out.put(Constants.STATS_NUM_PARTITIONS, partitionStats.size());
		out.put(Constants.STATS_NUM_READONLY_PARTITIONS, partitionStats.values().stream()
				.mapToInt(e -> (Boolean) e.get(Constants.STATS_IS_READONLY_PARTITION) ? 1 : 0).sum());
		return out;
	}

	@Override
	public void saveAsJson(String saveFileName, String cubeName) {
		Genson g = new Genson();
		try {
			PrintStream p = new PrintStream(
					new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(saveFileName))));
			partitions.entrySet().stream()
					.flatMap((e) -> e.getValue().asDataRowStream().peek((row) -> row.setPartition(e.getKey())))
					.peek((row) -> row.setCubeName(cubeName)).map((row) -> g.serialize(row))
					.forEach((rowString) -> p.println(rowString));
			p.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
