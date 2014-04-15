package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.Collection;
import java.util.Collections;
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
 * @author Fredo Erxleben
 * 
 */
public abstract class Stage<InType, OutType> implements Callable<StageResult> {

	protected boolean running = true;
	protected boolean finished = false;
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

	@Override
	public abstract StageResult call() throws Exception;

}
