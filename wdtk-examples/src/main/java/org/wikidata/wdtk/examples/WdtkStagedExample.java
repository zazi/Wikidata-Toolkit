package org.wikidata.wdtk.examples;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.WmfDumpFileManager;
import org.wikidata.wdtk.dumpfiles.parallel.FileFetchStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;
import org.wikidata.wdtk.util.DirectoryManager;
import org.wikidata.wdtk.util.DirectoryManagerImpl;
import org.wikidata.wdtk.util.WebResourceFetcher;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;

public class WdtkStagedExample {

	static StageManager manager = new StageManager();

	public static void main(String[] args) throws IOException {

		BlockingQueue<MwDumpFile> inputFiles = new LinkedBlockingQueue<>();

		FileFetchStage fileFetcher = new FileFetchStage();

		fileFetcher.addProducer(inputFiles);

		// fill the queues
		WmfDumpFileManager dumpfileManager = createDumpFileManager();
		inputFiles.addAll(dumpfileManager.findAllRelevantDumps(true));

		manager.submitStage(fileFetcher);

		manager.run();

		// NOTE need to give the stages some time before calling shutdown
		// this will be more comfortable if the stage lifecycle is handled by
		// the
		// stage manager

		while (!manager.collectResults()) {
			synchronized (Thread.currentThread()) {
				try {
					Thread.currentThread().wait(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		manager.shutdown();

		

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
