package org.wikidata.wdtk.dumpfiles.parallel;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;

public class FileFetchStage extends ContextFreeStage<MwDumpFile, MwDumpFile> {
	
	private Logger logger = LoggerFactory.getLogger(FileFetchStage.class);
	
	// TODO maybe later stages need more context information?
	
	public FileFetchStage(){
		this.waitTime = 1000;
		this.result = new CounterStageResult();
		
	}
	
	@Override
	protected void afterStep(){}
	
	@Override
	public MwDumpFile processElement(MwDumpFile element) {
		
		logger.info("Processing " + element.getProjectName() + " :: " + element.getDateStamp());
		
		try {
			// TODO unclutter this call into several useful calls
			// this downloads the element if neccessary
			element.prepareDumpFile(); 
			
			((CounterStageResult) this.result).increment();
			System.out.println("done");
			return element;
			
		} catch (IOException e) {
			// XXX for now… log them later on
			e.printStackTrace();
		}
		return null;
	}

}
