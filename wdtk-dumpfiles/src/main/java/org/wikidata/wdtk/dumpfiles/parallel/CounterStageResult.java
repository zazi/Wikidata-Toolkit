package org.wikidata.wdtk.dumpfiles.parallel;

public class CounterStageResult extends StageResult {

	private long counter = 0;
	
	public void increment(){
		counter++;
	}

	@Override
	public String getReport() {
		return "Counted " + this.counter;
	}
	
	
}
