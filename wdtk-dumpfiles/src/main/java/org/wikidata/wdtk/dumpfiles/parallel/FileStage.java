package org.wikidata.wdtk.dumpfiles.parallel;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import org.wikidata.wdtk.dumpfiles.WmfDumpFile;
import org.wikidata.wdtk.dumpfiles.WmfDumpFileManager;
import org.wikidata.wdtk.util.DirectoryManager;
import org.wikidata.wdtk.util.DirectoryManagerImpl;
import org.wikidata.wdtk.util.WebResourceFetcher;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;

public class FileStage extends Stage<WmfDumpFile, InputStream> {

	private DirectoryManager downloadDirectoryManager;
	private WebResourceFetcher webResourceFetcher = new WebResourceFetcherImpl();
	WmfDumpFileManager fileManager;
	
	public FileStage(){
		this.consumers = new LinkedList<>();
		this.producers = new LinkedList<>();
		this.waitTime = 1000;
		
		try {
			downloadDirectoryManager = new DirectoryManagerImpl(System.getProperty("user.dir"));
			fileManager = new WmfDumpFileManager("wikidatawiki", 
					downloadDirectoryManager, webResourceFetcher);
		} catch (IOException e) {
			e.printStackTrace();
			this.running = false;
			this.result = new FailedStageResult();
		}
		
		
	}
	
	@Override
	public InputStream processElement(WmfDumpFile element) {
		// TODO Auto-generated method stub
		return null;
	}

}
