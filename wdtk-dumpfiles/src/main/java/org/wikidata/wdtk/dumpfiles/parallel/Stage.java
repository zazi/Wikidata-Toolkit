package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A stage batch-processes all elements provided by producers. The results of
 * the processing are given to consumers. The life cycle of a stage starts when
 * calling the call()-method. Then the stage will call process() upon all
 * elements provided by all producers in the order they arrived in the input queue. 
 * Once all elements available at the point of calling process() are done, the stage waits for new elements. A stage can be signaled that there will be
 * no more elements to process. It then will process all remaining elements and
 * return a StageResult.
 * 
 * @author Fredo Erxleben
 * 
 */
public abstract class Stage<InType, OutType> implements Callable<StageResult> {

	protected boolean running = true;
	protected boolean finished = false;
	protected int waitTime = 100; // in usec

	protected StageResult result;
	protected Collection<Stage<?, InType>> producers = new HashSet<>();
	protected Collection<Stage<OutType, ?>> consumers = new HashSet<>();
	
	protected BlockingQueue<InType> inputQueue = new LinkedBlockingQueue<InType>();

	public synchronized void addProducer(Stage<?, InType> producer) {
		this.producers.add(producer);
	}

	public synchronized void addConsumer(Stage<OutType, ?> consumer) {
		this.consumers.add(consumer);
	}
	
	/**
	 * Receive an element into the input queue to be processed later on.
	 * @param input
	 */
	public synchronized void addInput(InType input){
		this.inputQueue.add(input);
	}

	/**
	 * Distribute a processed element to all registered consumers.
	 * The element is always added into their input queues.
	 * @param myOutput
	 */
	protected void distribute(OutType myOutput){
		for(Stage<OutType, ?> s : this.consumers){
			s.addInput(myOutput);
			synchronized(s){
				s.notify();
			}
		}
	}
	
	public synchronized boolean isFinished(){
		return this.finished;
	}
	
	public abstract OutType processElement(InType element);

	@Override
	public abstract StageResult call() throws Exception;

	@Override
	public String toString(){
		return this.getClass().toString();
	}
}
