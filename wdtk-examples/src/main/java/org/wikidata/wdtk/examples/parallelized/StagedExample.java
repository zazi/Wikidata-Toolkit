package org.wikidata.wdtk.examples.parallelized;

import org.wikidata.wdtk.dumpfiles.parallel.CollectorStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;
import org.wikidata.wdtk.examples.ExampleHelpers;

/**
 * This example demonstrates how to set up a simple workflow with the staging
 * concept.
 * 
 * @author Fredo Erxleben
 * 
 */
public class StagedExample {

	static StageManager manager = new StageManager();

	public static void main(String[] args) {

		ExampleHelpers.configureLogging();

		// setup the stages
		CollectorStage<Integer> collector = new CollectorStage<Integer>();
		SquaringStage squarer = new SquaringStage();
		PrintingStage<Integer> printer = new PrintingStage<>();

		// define who is connected to whom
		// note that connected stages are submitted automatically
		manager.connectStages(squarer, collector);
		manager.connectStages(collector, printer);

		// fill the queues
		for (int i = 0; i < 1000; i++) {
			squarer.addInput(i);
			collector.addInput(i);

		}

		// run the setup
		manager.run();

		// wait until it finishes
		manager.waitForFuture();

		// once finished, print the stage reports
		for (StageResult r : manager.getStageResults()) {
			System.out.println(r.getId() + ": " + r.getReport());
		}
	}

}
