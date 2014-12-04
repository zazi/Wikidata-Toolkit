package org.wikidata.wdtk.dumpfiles.wmf;

import org.wikidata.wdtk.dumpfiles.MwDumpFileMetaData;

public class WmfDumpFileMetadata implements MwDumpFileMetaData {
	
	private final String dateStamp;
	private final String projectName;
	
	public WmfDumpFileMetadata(String dateStamp, String projectName){
		this.dateStamp = dateStamp;
		this.projectName = projectName;
	}
	
	@Override
	public String getProjectName() {
		return this.projectName;
	}

	@Override
	public String getDateStamp() {
		return this.dateStamp;
	}

}
