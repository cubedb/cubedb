package org.cubedb.core;

import java.io.BufferedInputStream;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.cubedb.core.beans.DataRow;
import org.cubedb.core.beans.Filter;
import org.cubedb.core.beans.Pair;
import org.cubedb.core.beans.SearchResult;
import org.cubedb.core.beans.SearchResultRow;
import org.cubedb.offheap.OffHeapPartition;
import org.cubedb.utils.CubeUtils;
import org.cubedb.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;
import com.owlike.genson.Genson;

public class CubeImpl implements Cube {
	public static final Logger log = LoggerFactory.getLogger(CubeImpl.class);
	Map<String, Partition> partitions;
	AtomicInteger recordsCount;
	String partitionColumn;

	public CubeImpl(String partitionColumn) {
		partitions = new ConcurrentHashMap<String, Partition>();
		this.partitionColumn = partitionColumn;
		this.recordsCount = new AtomicInteger(0);
	}

	private Partition createNewPartition(String partitionName) {
		//Partition p = new CachedCountersOffHeapPartition();
		Partition p = new OffHeapPartition();
		return p;
	}

	protected void insert(Collection<String> partitions, Map<String, List<DataRow>> groupedData) {
		for (String p : partitions) {
			Partition partition = this.partitions.computeIfAbsent(p, this::createNewPartition);
			for (DataRow d : groupedData.get(p)) {
				partition.insert(d);
				recordsCount.incrementAndGet();
			}
		}
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
				log.error("Interruppt");
			}
		}
	}

	public void insert(List<DataRow> data) {
		Map<String, List<DataRow>> groupedData = data.stream().collect(Collectors.groupingBy(DataRow::getPartition));
		// insertSequential(groupedData);
		//log.debug("The following partitions detected: {}", groupedData.keySet());
		if (data.size() < 1500 || groupedData.size() <= 3)
			insertSequential(groupedData);
		else
			insertParallel(groupedData);
	}

	protected Map<SearchResultRow, MutableLong> get(List<Pair<String, Partition>> partitions, List<Filter> filters,
			String fromPartition, String toPartition) {
		Map<SearchResultRow, MutableLong> out = new HashMap<SearchResultRow, MutableLong>();
		for (Pair<String, Partition> e : partitions) {
			String partitionValue = e.getKey();
			boolean partitionMatch = partitionValue.compareTo(fromPartition) >= 0
					&& partitionValue.compareTo(toPartition) <= 0;

			SearchResult searchResult = e.getValue().get(filters);
			if (partitionMatch) {
				for (Entry<SearchResultRow, Long> sr : searchResult.getResults().entrySet()) {
					MutableLong c = out.get(sr.getKey());
					if (c == null) {
						c = new MutableLong();
						out.put(sr.getKey(), c);
					}
					c.increment(sr.getValue());
				}
			}
			for (Entry<String, Long> tc : searchResult.getTotalCounts().entrySet()) {
				SearchResultRow r = new SearchResultRow(partitionColumn, partitionValue, tc.getKey());
				out.put(r, new MutableLong(tc.getValue()));
			}
		}
		return out;
	}

	class Searcher implements Runnable {
		final private List<Pair<String, Partition>> partitions;
		private Map<SearchResultRow, MutableLong> result;
		final private List<Filter> filters;
		final private String fromPartition, toPartition;

		public Searcher(List<Pair<String, Partition>> partitions, List<Filter> filters, String fromPartition,
				String toPartition) {
			this.partitions = partitions;
			this.filters = filters;
			this.fromPartition = fromPartition;
			this.toPartition = toPartition;
		}

		@Override
		public void run() {
			this.result = get(partitions, filters, fromPartition, toPartition);
		}

		public Map<SearchResultRow, MutableLong> getResult() {
			return this.result;
		};

	}

	@Override
	public Map<SearchResultRow, Long> get(final String fromPartition, final String toPartition, List<Filter> filters) {
		long t0 = System.currentTimeMillis();
		List<Pair<String, Partition>> partitions = this.partitions.entrySet().stream()
				.filter((e) -> e.getKey().compareTo(fromPartition) >= 0 && e.getKey().compareTo(toPartition) <= 0)
				.map(e -> new Pair<String, Partition>(e.getKey(), e.getValue())).collect(Collectors.toList());
		List<Filter> realFilters = filters.stream().filter((f) -> !f.getField().equals(this.partitionColumn))
				.collect(Collectors.toList());
		List<Filter> partitionFilters = filters.stream().filter((f) -> f.getField().equals(this.partitionColumn))
				.collect(Collectors.toList());

		final String fromPartitionFilter = partitionFilters.stream()
				.map(f -> Arrays.stream(f.getValues()).min(String::compareTo).get()).min(String::compareTo)
				.orElse(fromPartition);
		final String toPartitionFilter = partitionFilters.stream()
				.map(f -> Arrays.stream(f.getValues()).max(String::compareTo).get()).max(String::compareTo)
				.orElse(toPartition);
		Collections.shuffle(partitions);
		final List<List<Pair<String, Partition>>> partitionSlices = CubeUtils.partitionList(partitions);
		 //log.info("Partitions are distributed in this way: {}",
		 //partitionSlices.stream().map( s -> s.stream().map(p ->
		 //p.getT()).collect(Collectors.toList()).toString()).collect(Collectors.toList()));
		Searcher[] searchers = new Searcher[partitionSlices.size()];
		Thread[] threads = new Thread[partitionSlices.size()];
		for (int i = 0; i < searchers.length; i++) {
			searchers[i] = new Searcher(partitionSlices.get(i), realFilters, fromPartitionFilter, toPartitionFilter);
			threads[i] = new Thread(searchers[i]);
			threads[i].start();
		}
		try {
			for (int i = 0; i < searchers.length; i++)
				threads[i].join();

		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		Map<SearchResultRow, MutableLong> result = new HashMap<SearchResultRow, MutableLong>();
		for (int i = 0; i < searchers.length; i++) {
			for (Entry<SearchResultRow, MutableLong> e : searchers[i].getResult().entrySet())
				result.computeIfAbsent(e.getKey(), row -> new MutableLong()).increment(e.getValue().get());
		}
		long t1 = System.currentTimeMillis();
		return result.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
	}

	@Override
	public int getNumRecords() {
		return recordsCount.get();
	}

	@Override
	public void deletePartition(String partitionName) {
		this.partitions.remove(partitionName);

	}

	@Override
	public Map<SearchResultRow, Long> get(int lastRange, List<Filter> filters) {
		List<String> partitions = this.partitions.keySet().stream().sorted((e, ot) -> ot.compareTo(e)).limit(lastRange)
				.collect(Collectors.toList());
		if (partitions.size() == 0) {
			return new HashMap<SearchResultRow, Long>();
		}
		final String toPartition = partitions.get(0);
		final String fromPartition = partitions.get(partitions.size() - 1);

		return this.get(fromPartition, toPartition, filters);
	}

	@Override
	public TreeSet<String> getPartitions(final String from, final String to) {
		return this.partitions.keySet().stream().filter(p -> from == null || from.compareTo(p) <= 0)
				.filter(p -> from == null || to.compareTo(p) >= 0).sorted()
				.collect(Collectors.toCollection(() -> new TreeSet<String>()));
	}

	@Override
	public void save(String saveFileName) throws IOException {
		Kryo kryo = CubeUtils.getKryoWithRegistrations();
		OutputStream zip = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(saveFileName)));
		Output output = new Output(zip);
		kryo.writeClassAndObject(output, this.partitions);
		// zip.closeEntry();
		output.close();
		// zip.close();
	}

	@Override
	public void load(String saveFileName) throws IOException {
		Kryo kryo = CubeUtils.getKryoWithRegistrations();
		Log.TRACE();
		InputStream zip = new GZIPInputStream(new BufferedInputStream(new FileInputStream(saveFileName)));
		Input input = new Input(zip);
		this.partitions = (Map<String, Partition>) kryo.readClassAndObject(input);
		input.close();
		System.gc();
		// zip.close();
	}

	@Override
	public Map<String, Object> getStats() {
		Map<String, Map<String, Object>> partitionStats = this.partitions.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getStats()));
		Map<String, Object> out = new HashMap<String, Object>();
		//out.put("partitionStats", partitionStats);
		out.put(Constants.STATS_COLUMN_SIZE, partitionStats.values().stream().mapToLong(e -> (Long)e.get(Constants.STATS_COLUMN_SIZE)).sum());
		out.put(Constants.STATS_METRIC_SIZE, partitionStats.values().stream().mapToLong(e -> (Long)e.get(Constants.STATS_METRIC_SIZE)).sum());
		out.put(Constants.STATS_COLUMN_BLOCKS, partitionStats.values().stream().mapToInt(e -> (Integer)e.get(Constants.STATS_COLUMN_BLOCKS)).sum());
		out.put(Constants.STATS_METRIC_BLOCKS, partitionStats.values().stream().mapToInt(e -> (Integer)e.get(Constants.STATS_METRIC_BLOCKS)).sum());
		out.put(Constants.STATS_NUM_RECORDS, partitionStats.values().stream().mapToInt(e -> (Integer)e.get(Constants.STATS_NUM_RECORDS)).sum());
		out.put(Constants.STATS_NUM_LARGE_BLOCKS, partitionStats.values().stream().mapToInt(e -> (Integer)e.get(Constants.STATS_NUM_LARGE_BLOCKS)).sum());
		out.put(Constants.STATS_NUM_PARTITIONS, partitionStats.size());
		out.put(Constants.STATS_NUM_READONLY_PARTITIONS, partitionStats.values().stream().mapToInt(e -> (Integer)e.get(Constants.STATS_IS_READONLY_PARTITION)).sum());
		return out;
	}
	
	@Override
	public void saveAsJson(String saveFileName, String cubeName){
		Genson g = new Genson();
		try {
			PrintStream p = new PrintStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(saveFileName))));
			this.partitions
				.entrySet()
				.stream()
				.flatMap( (e) -> e.getValue()
							.asDataRowStream()
							.peek((row) -> row.setPartition(e.getKey()))
						)
				.peek( (row) -> row.setCubeName(cubeName))
				.map( (row) -> g.serialize(row))
				.forEach((rowString) -> p.println(rowString));
			p.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
