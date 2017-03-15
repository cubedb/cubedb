package org.cubedb.stats;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.cubedb.core.Constants;
import org.cubedb.core.MultiCube;
import org.cubedb.core.beans.DataRow;

import jersey.repackaged.com.google.common.collect.ImmutableMap;

public class CubeStatsSender implements StatsSender {

	private MultiCube multiCube;
	private final String CUBE_NAME = "system_stats";
	private final DateFormat daily = new SimpleDateFormat("yyyy-MM-dd");
	private final DateFormat hourly = new SimpleDateFormat("yyyy-MM-dd hh");

	public CubeStatsSender(MultiCube multiCube) {
		this.multiCube = multiCube;
	}

	@Override
	public void send(String action, String cubeName, boolean groupBy, boolean error) {
		if(!Constants.sendStats)
			return;
		Date now = new Date();
		Map<String, String> fields = ImmutableMap.of("action", action, "cube_name", cubeName, "group_by", Boolean.toString(groupBy), "is_error", Boolean.toString(error));
		Map<String, Long> counters = ImmutableMap.of("c", 1l);
		List<DataRow> data = new ArrayList<DataRow>(2);
		DataRow dailyStats = new DataRow();
		dailyStats.setCubeName(CUBE_NAME + "_day");
		dailyStats.setFields(fields);
		dailyStats.setCounters(counters);
		dailyStats.setPartition(daily.format(now));
		data.add(dailyStats);
		DataRow hourlyStats = new DataRow();
		hourlyStats.setCubeName(CUBE_NAME + "_hourly");
		hourlyStats.setFields(fields);
		hourlyStats.setCounters(counters);
		hourlyStats.setPartition(hourly.format(now));
		data.add(hourlyStats);
		multiCube.insert(data);
	}

	@Override
	public void send(String action) {
		send(action, null, false, false);

	}

}
