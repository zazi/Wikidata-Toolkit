package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * A stage batch-processes all elements provided by producers. The results of
 * the processing are given to consumers. The life cycle of a stage starts when
 * calling the call()-method. Then the stage will call process() upon all
 * elements provided by all consumers in order. Once all elements are depleted
 * the stage waits for new elements. A stage can be signaled that there will be
 * no more elements to process. It then will process all remaining elements and
 * return a StageResult.
 * 
 * @author fredo
 * 
 */
public abstract class Stage<InType, OutType> implements Callable<StageResult> {

	protected boolean running = true;
	private boolean finished = false;
	protected int waitTime = 100; // in usec

	protected StageResult result;
	protected Collection<BlockingQueue<InType>> producers = Collections.emptyList();
	protected Collection<BlockingQueue<OutType>> consumers = Collections.emptyList();

	public synchronized boolean addProducer(BlockingQueue<InType> producer) {
		this.producers.add(producer);
		return true;
	}

	public synchronized boolean addConsumer(BlockingQueue<OutType> consumer) {
		this.consumers.add(consumer);
		return true;
	}

	public synchronized void finish(){
		this.running = false;
	}
	
	public synchronized boolean isFinished(){
		return this.finished;
	}
	
	public abstract OutType processElement(InType element);

	/**
	 * The default implementation processes the input only element-wise
	 */
	@Override
	public StageResult call() throws Exception {
		List<InType> currentStep = new LinkedList<>();

		while (this.running) {
			// get all the input for the steps
			for (BlockingQueue<InType> producer : this.producers) {
				producer.drainTo(currentStep);
			}
			// process the elements
			while (!currentStep.isEmpty()) {
				OutType stepResult = this.processElement(currentStep.remove(0));
				// distribute result to all consumers
				for (BlockingQueue<OutType> consumer : this.consumers) {
					consumer.put(stepResult);
				}
			}
			
			// wait for new input
			synchronized(this){
				wait(this.waitTime);
			}
		}
		this.finished = true;
		return this.result;
	}

}
