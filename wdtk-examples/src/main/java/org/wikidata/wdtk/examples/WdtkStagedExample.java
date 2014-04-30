package org.wikidata.wdtk.examples;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.WmfDumpFileManager;
import org.wikidata.wdtk.dumpfiles.parallel.FileFetchStage;
import org.wikidata.wdtk.dumpfiles.parallel.RevisionProcessingStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;
import org.wikidata.wdtk.util.DirectoryManager;
import org.wikidata.wdtk.util.DirectoryManagerImpl;
import org.wikidata.wdtk.util.Timer;
import org.wikidata.wdtk.util.WebResourceFetcher;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;

public class WdtkStagedExample {

	static StageManager manager = new StageManager();
	static Timer timer = new Timer("ProcessingExample", Timer.RECORD_ALL);

	public static void main(String[] args) throws IOException {
		
		configureLogging();

		
		FileFetchStage fileFetcher = new FileFetchStage();
		RevisionProcessingStage revisionProcessor = new RevisionProcessingStage();

		manager.submitStage(fileFetcher);
		manager.submitStage(revisionProcessor);
		manager.connectStages(fileFetcher, revisionProcessor);

		// fill the queues
		WmfDumpFileManager dumpfileManager = createDumpFileManager();
		for(MwDumpFile file : dumpfileManager.findAllRelevantDumps(true)){
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
	
	/**
	 * Defines how messages should be logged. This method can be modified to
	 * restrict the logging messages that are shown on the console or to change
	 * their formatting. See the documentation of Log4J for details on how to do
	 * this.
	 */
	private static void configureLogging() {
		// Create the appender that will write log messages to the console.
		ConsoleAppender consoleAppender = new ConsoleAppender();
		// Define the pattern of log messages.
		// Insert the string "%c{1}:%L" to also show class name and line.
		String pattern = "%d{yyyy-MM-dd HH:mm:ss} %-5p - %m%n";
		consoleAppender.setLayout(new PatternLayout(pattern));
		// Change to Level.ERROR for fewer messages:
		consoleAppender.setThreshold(Level.INFO);

		consoleAppender.activateOptions();
		Logger.getRootLogger().addAppender(consoleAppender);
	}
}
