package org.wikidata.wdtk.examples;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.wikidata.wdtk.dumpfiles.parallel.CollectorStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;

public class StagedExample {

	
	static StageManager manager = new StageManager();
	
	public static void main(String[] args){
		
		configureLogging();
		
		// setup the stages
		CollectorStage<Integer> collector = new CollectorStage<Integer>();
		SquaringStage squarer = new SquaringStage();
		PrintingStage<Integer> printer = new PrintingStage<>();
		
		// define who is connected to whom
		// note that connected stages are submitted automatically
		manager.connectStages(squarer, collector);
		manager.connectStages(collector, printer);
		
		// fill the queues
		for(int i = 0; i < 1000; i++){
			squarer.addInput(i);
			collector.addInput(i);

		}
		
		// run the setup
		manager.run();
		
		// wait until it finishes
		manager.waitForFuture();
		
		// once finished, print the stage reports
		for(StageResult r : manager.getStageResults()){
			System.out.println(r.getId() + ": " + r.getReport());
		}
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
