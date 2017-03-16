package org.cubedb.stats;

import java.util.Collection;

public interface StatsSender {
	final String CUBE_NAME = "system_stats";
	final String FLAG_FIELD_VALUE = "set";
	public void send(String action);
	void send(String action, String cubeName, boolean groupBy, boolean error, Collection<String> flags);
}
