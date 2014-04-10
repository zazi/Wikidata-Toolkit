package org.wikidata.wdtk.dumpfiles.parallel;



public abstract class StageResult {

	static long nextId = 0;
	
	private long id;
	
	StageResult(){
		this.id = nextId;
		nextId++;
	}
	
	public long getId(){
		return this.id;
	}
	
	public abstract String getReport();
}
