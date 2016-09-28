package org.cubedb.offheap;

import org.cubedb.api.KeyMap;



public class DummyKeyMap implements KeyMap {

	
	public DummyKeyMap(){
		this.createMap(0, 1);
	}
	
	public DummyKeyMap(int curSize, int fieldsLength){
		this.createMap(0, 1);
	}

	
	protected void createMap(int curSize, int fieldsLength)
	{
		
		
	}
	
	@Override
	public Integer get(byte[] b) {
		return null;
	}

	@Override
	public void put(byte[] k, int v) {
		
	}

	@Override
	public int size() {
		return 0;
	}

}
