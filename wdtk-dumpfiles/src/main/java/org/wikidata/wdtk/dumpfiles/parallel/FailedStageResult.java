package org.wikidata.wdtk.dumpfiles.parallel;

/**
 * Represents a result of a stage that failed to execute properly.
 * @author Fredo Erxleben
 *
 */
public class FailedStageResult extends StageResult {
	
	// TODO allow submitting a reason
	
	@Override
	public String getReport() {
		return "Stage execution failed";
	}

}
