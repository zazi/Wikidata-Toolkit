package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

/**
 * A collector stage is a stage that collects the results of multiple producers
 * and merges them into one result.
 * The type of the results does not change during collection.
 * 
 * @author fredo
 * 
 */
public class CollectorStage<Type> extends Stage<Type, Type> {
// NOTE this would be a good place to apply filtering and sorting
	
	public CollectorStage(){
		this.result = new CounterStageResult();
		this.producers = new LinkedList<>();
	}
	
	/**
	 * Only one consumer can be added.
	 * On the attempt to add another consumer the method will return false.
	 */
	@Override
	public  synchronized boolean addConsumer(BlockingQueue<Type> consumer){
		if(this.consumers.isEmpty()){
			this.consumers = Collections.singletonList(consumer);
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public Type processElement(Type element) {
		// filter or sorting here
		// sorting might need to override call()
		((CounterStageResult) this.result).increment();
		return element;
	}

}
