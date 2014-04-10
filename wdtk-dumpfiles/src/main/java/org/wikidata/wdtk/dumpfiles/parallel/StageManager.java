package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class StageManager {
	
	// TODO the whole life cycle should be managed by this class

	ExecutorService executor = Executors.newCachedThreadPool();
	
	List<Stage<?,?>> stages = new LinkedList<>();
	List<Future<StageResult>> futures = new LinkedList<>();
	List<StageResult> results = new LinkedList<>();
	List<BlockingQueue<?>> connectors = new LinkedList<>();
	
	/**
	 * 
	 * @param sender
	 * @param receiver
	 * @return
	 */
	public <CommonType> BlockingQueue<CommonType> connectStages(Stage<?, CommonType> sender, Stage<CommonType, ?> receiver){
		
		BlockingQueue<CommonType> connectionQueue = new LinkedBlockingQueue<CommonType>();
		
		sender.addConsumer(connectionQueue);
		receiver.addProducer(connectionQueue);
		
		this.connectors.add(connectionQueue);
		
		return connectionQueue;
	}
	
	public void submitStage(Stage<?,?> toLaunch){
		
		stages.add(toLaunch);
	}

	public void run(){
		for(Stage<?,?> stage : this.stages){
			Future<StageResult> result = executor.submit(stage);
			this.futures.add(result);
		}
	}
	
	public void collectResults(){
		List<Future<StageResult>> toRemove = new LinkedList<>();
		
		for(Future<StageResult> future : this.futures){
			if(future.isDone()){
				try {
					toRemove.add(future);
					this.results.add(future.get());
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		
		this.futures.removeAll(toRemove);
	}
	
	/**
	 * The StageResults are kept for evaluation.
	 * The Futures and Stages and connecting Queues will be cleared.
	 */
	public void shutdown(){
		this.executor.shutdown();
		
		for(Stage<?,?> stage : this.stages){
			stage.finish();
		}
		
		for(BlockingQueue<?> queue : this.connectors){
			queue.clear();
		}
		
		for(Future<StageResult> future : this.futures){
			try {
				this.results.add(future.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		this.stages.clear();
		this.futures.clear();
		this.connectors.clear();
	}

	public List<StageResult> getStageResults(){
		return this.results;
	}
}
