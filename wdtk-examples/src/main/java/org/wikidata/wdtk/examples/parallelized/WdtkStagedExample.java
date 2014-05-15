package org.wikidata.wdtk.examples.parallelized;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.WmfDumpFileManager;
import org.wikidata.wdtk.dumpfiles.parallel.DecompressorStage;
import org.wikidata.wdtk.dumpfiles.parallel.FileFetchStage;
import org.wikidata.wdtk.dumpfiles.parallel.RevisionProcessingStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;
import org.wikidata.wdtk.examples.ExampleHelpers;
import org.wikidata.wdtk.util.DirectoryManager;
import org.wikidata.wdtk.util.DirectoryManagerImpl;
import org.wikidata.wdtk.util.Timer;
import org.wikidata.wdtk.util.WebResourceFetcher;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;

public class WdtkStagedExample {

	static StageManager manager = new StageManager();
	static Timer timer = new Timer("ProcessingExample", Timer.RECORD_ALL);

	public static void main(String[] args) throws IOException {
		
		ExampleHelpers.configureLogging();
		
		FileFetchStage fileFetcher = new FileFetchStage();
		DecompressorStage decompressor = new DecompressorStage();
		// RevisionProcessingStage revisionProcessor = new RevisionProcessingStage(); // todo deprecated

		manager.submitStage(fileFetcher); // NOTE redundant
		// manager.submitStage(revisionProcessor); // NOTE redundant
		manager.connectStages(fileFetcher, decompressor);
		// TODO complete chain

		// fill the queues
		WmfDumpFileManager dumpfileManager = createDumpFileManager();
		for(MwDumpFile file : dumpfileManager.findAllRelevantRevisionDumps(true)){
			fileFetcher.addInput(file);
		}

		manager.run();
		
		// here you could do something else
		
		manager.waitForFuture();

		for (StageResult r : manager.getStageResults()) {
			System.out.println(r.getId() + ": " + r.getReport());
		}
	}

	private static WmfDumpFileManager createDumpFileManager()
			throws IOException {
		// The following can also be set to another directory:
		String downloadDirectory = System.getProperty("user.dir");
		DirectoryManager downloadDirectoryManager = new DirectoryManagerImpl(
				downloadDirectory);

		// The following can be set to null for offline operation:
		WebResourceFetcher webResourceFetcher = new WebResourceFetcherImpl();

		// The string "wikidatawiki" identifies Wikidata.org:
		return new WmfDumpFileManager("wikidatawiki", downloadDirectoryManager,
				webResourceFetcher);
	}
}
