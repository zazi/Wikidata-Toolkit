package org.wikidata.wdtk.dumpfiles.parallel;

import java.io.IOException;
import java.util.LinkedList;

import org.wikidata.wdtk.dumpfiles.MwDumpFile;

public class FileFetchStage extends ContextFreeStage<MwDumpFile, MwDumpFile> {
	
	// TODO maybe later stages need more context information?
	
	public FileFetchStage(){
		this.consumers = new LinkedList<>();
		this.producers = new LinkedList<>();
		this.waitTime = 1000;
		this.result = new CounterStageResult();
		
	}
	
	@Override
	protected void afterStep(){
		// finish, since there will be no additional input
		this.finish();
	}
	
	@Override
	public MwDumpFile processElement(MwDumpFile element) {
		
		System.out.println("Processing " + element.getProjectName() + " :: " + element.getDateStamp());
		
		try {
			// TODO unclutter this call into several useful calls
			// this downloads the element if neccessary
			element.prepareDumpFile(); 
			
			((CounterStageResult) this.result).increment();
			System.out.println("done");
			return element;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
