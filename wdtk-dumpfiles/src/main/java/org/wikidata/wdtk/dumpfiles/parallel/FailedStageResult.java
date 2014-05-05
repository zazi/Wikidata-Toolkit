package org.wikidata.wdtk.dumpfiles.parallel;

/**
 * Represents a result of a stage that failed to execute properly.
 * @author Fredo Erxleben
 *
 */
public class FailedStageResult extends StageResult {
	
	private String reason;
	private Exception exception;
	
	public FailedStageResult(Exception exception){
		this.exception = exception;
		this.reason = exception.getMessage();
	}
	
	public FailedStageResult(){
		this.exception = null;
		this.reason = "No reason given.";
	}
	
	public Exception getException(){
		return this.exception;
	}
	
	@Override
	public String getReport() {
		return "Stage execution failed.\n" + this.reason;
	}

}
