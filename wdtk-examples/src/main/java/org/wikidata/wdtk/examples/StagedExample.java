package org.wikidata.wdtk.examples;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.wikidata.wdtk.dumpfiles.parallel.CollectorStage;
import org.wikidata.wdtk.dumpfiles.parallel.StageManager;
import org.wikidata.wdtk.dumpfiles.parallel.StageResult;

public class StagedExample {

	
	static StageManager manager = new StageManager();
	
	public static void main(String[] args){
		
		BlockingQueue<Integer> input0 = new LinkedBlockingQueue<>();
		BlockingQueue<Integer> input1 = new LinkedBlockingQueue<>();
		BlockingQueue<Integer> output = new LinkedBlockingQueue<>();
		
		CollectorStage<Integer> collector = new CollectorStage<Integer>();
		SquaringStage squarer = new SquaringStage();
		
		squarer.addProducer(input0);
		
		manager.connectStages(squarer, collector);
		
		collector.addProducer(input1);
		collector.addConsumer(output);
		
		// fill the queues
		for(int i = 0; i < 1000; i++){
			input0.add(i);
			input1.add(i);
		}
		
		manager.submitStage(collector);
		manager.submitStage(squarer);
		
		manager.run();
		
		// NOTE need to give the stages some time before calling shutdown
		// this will be more comfortable if the stage lifecycle is handled by the
		// stage manager
		synchronized(Thread.currentThread()){
		try {
			Thread.currentThread().wait(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
		manager.shutdown();
		manager.collectResults();
		
		for(StageResult r : manager.getStageResults()){
			System.out.println(r.getId() + ": " + r.getReport());
		}
	}
}
