package org.wikidata.wdtk.examples;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

		timer.start();
		BlockingQueue<MwDumpFile> inputFiles = new LinkedBlockingQueue<>();
		BlockingQueue<MwDumpFile> downloadedFiles = new LinkedBlockingQueue<>();
		
		FileFetchStage fileFetcher = new FileFetchStage();
		RevisionProcessingStage revisionProcessor = new RevisionProcessingStage();

		fileFetcher.addProducer(inputFiles);
		fileFetcher.addConsumer(downloadedFiles);
		revisionProcessor.addProducer(downloadedFiles);

		// fill the queues
		WmfDumpFileManager dumpfileManager = createDumpFileManager();
		inputFiles.addAll(dumpfileManager.findAllRelevantDumps(true));

		manager.submitStage(fileFetcher);
		manager.submitStage(revisionProcessor);
		
		timer.stop();
		System.out.println("Setup time: " + timer.toString());
		timer.start();

		manager.run();

		// NOTE need to give the stages some time before calling shutdown
		// this will be more comfortable if the stage lifecycle is handled by
		// the
		// stage manager

		while (!manager.canBeCollected(fileFetcher)) {
			synchronized (Thread.currentThread()) {
				try {
					Thread.currentThread().wait(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		while (!downloadedFiles.isEmpty()) {
			synchronized (Thread.currentThread()) {
				try {
					Thread.currentThread().wait(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		timer.stop();
		System.out.println("Time until shutdown: " + timer.toString());
		timer.start();
		
		manager.shutdown();
		
		timer.stop();
		System.out.println("Overall time: " + timer.toString());

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
