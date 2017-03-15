package org.cubedb.stats;

public interface StatsSender {

	public void send(String action, String cubeName, boolean groupBy, boolean error);
	public void send(String action);
}
