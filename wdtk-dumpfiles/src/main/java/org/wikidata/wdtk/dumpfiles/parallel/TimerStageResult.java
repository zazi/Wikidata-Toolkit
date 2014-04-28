package org.wikidata.wdtk.dumpfiles.parallel;

import org.wikidata.wdtk.util.Timer;

public class TimerStageResult extends StageResult {

	Timer timer = new Timer("Stage result timer " + this.getId(), Timer.RECORD_ALL);
	
	Timer getTimer(){
		return this.timer;
	}
	
	@Override
	public String getReport() {
		return this.timer.toString();
	}

}
