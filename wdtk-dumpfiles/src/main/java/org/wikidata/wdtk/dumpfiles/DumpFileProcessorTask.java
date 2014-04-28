package org.wikidata.wdtk.dumpfiles;

import java.io.InputStream;
import java.util.concurrent.Callable;

public class DumpFileProcessorTask implements Callable<Void> {

	MwDumpFileProcessor dumpFileProcessor;
	InputStream inputStream;
	MwDumpFile dumpFile;
	
	public DumpFileProcessorTask(MwDumpFileProcessor dumpFileProcessor, InputStream inputStream, MwDumpFile dumpFile) {
		this.dumpFileProcessor = dumpFileProcessor;
		this.inputStream = inputStream;
		this.dumpFile = dumpFile;
	}

	@Override
	public Void call() throws Exception {
		this.dumpFileProcessor.processDumpFileContents(inputStream, dumpFile);
		return null;
	}

}
