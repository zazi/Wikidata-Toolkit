package org.wikidata.wdtk.dumpfiles.parallel;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import org.wikidata.wdtk.dumpfiles.MwDumpFile;

public class FileFetchStage extends ContextFreeStage<MwDumpFile, InputStream> {
	
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
	public InputStream processElement(MwDumpFile element) {
		
		// TODO unclutter this call into several useful calls
		System.out.println("Processing " + element.getProjectName() + " :: " + element.getDateStamp());
		
		try {
			InputStream stream = element.getDumpFileStream(); 
			((CounterStageResult) this.result).increment();
			System.out.println("done");
			return stream;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
