package org.wikidata.wdtk.dumpfiles.parallel;

public class FailedStageResult extends StageResult {
	
	// TODO allow submitting a reason
	
	@Override
	public String getReport() {
		return "Stage execution failed";
	}

}
