package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class manages the life cycle of all processing stages and the
 * connections between these stages.
 * 
 * @author Fredo Erxleben
 * 
 */
public class StageManager {

	// TODO the whole life cycle should be managed by this class
	// TODO maybe the manager should get its own thread
	// TODO logging support

	ExecutorService executor = Executors.newCachedThreadPool();
	List<Stage<?, ?>> scheduledStages = new LinkedList<>();
	Map<Stage<?, ?>, Future<StageResult>> runningStages = new HashMap<>();
	List<StageResult> results = new LinkedList<>();
	List<BlockingQueue<?>> connectors = new LinkedList<>();

	/**
	 * 
	 * @param sender
	 * @param receiver
	 * @return
	 */
	public <CommonType> BlockingQueue<CommonType> connectStages(
			Stage<?, CommonType> sender, Stage<CommonType, ?> receiver) {

		BlockingQueue<CommonType> connectionQueue = new LinkedBlockingQueue<CommonType>();

		sender.addConsumer(connectionQueue);
		receiver.addProducer(connectionQueue);

		this.connectors.add(connectionQueue);

		return connectionQueue;
	}

	/**
	 * Submits a stage to the set of stages to be launched later.
	 * 
	 * @param toLaunch
	 */
	public void submitStage(Stage<?, ?> toLaunch) {

		this.scheduledStages.add(toLaunch);
	}

	/**
	 * Executes all earlier submitted stages. This hands the stages threads over
	 * to the
	 */
	public void run() {
		for (Stage<?, ?> stage : this.scheduledStages) {
			Future<StageResult> result = executor.submit(stage);
			this.runningStages.put(stage, result);
		}
	}

	/**
	 * Collects all available results.
	 * 
	 * @return true, if no more results are outstanding.
	 */
	public boolean collectResults() {
		List<Stage<?, ?>> toRemove = new LinkedList<>();

		for (Entry<Stage<?, ?>, Future<StageResult>> future : this.runningStages
				.entrySet()) {
			if (future.getValue().isDone()) {
				try {
					toRemove.add(future.getKey());
					this.results.add(future.getValue().get());
					System.out.println("Collecting Result");
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}

		for (Stage<?, ?> key : toRemove) {
			this.runningStages.remove(key);
		}

		return this.runningStages.isEmpty();
	}

	/**
	 * The StageResults are kept for evaluation. The Futures and Stages and
	 * connecting queues will be cleared. All scheduled stages are discarded.
	 */
	public void shutdown() {
		System.out.println("Shutting down manager");
		this.executor.shutdown();

		// tell all stages to finish gracefully
		// but do not await the results just yet, because they might take time
		// to finish
		for (Stage<?, ?> stage : this.runningStages.keySet()) {
			stage.finish();
		}

		for (BlockingQueue<?> queue : this.connectors) {
			queue.clear();
		}

		for (Future<StageResult> future : this.runningStages.values()) {
			try {
				this.results.add(future.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		this.scheduledStages.clear();
		this.runningStages.clear();
		this.connectors.clear();
	}

	/**
	 * Checks if a stage was running, but the future has not yet been collected.
	 * 
	 * @param stage
	 *            the stage to be checked for
	 * @return true if the stage was running and is done; false otherwise
	 */
	public boolean canBeCollected(Stage<?, ?> stage) {
		return (this.runningStages.containsKey(stage) && this.runningStages
				.get(stage).isDone());
	}

	/**
	 * 
	 * @return all stage results that were already collected
	 */
	public List<StageResult> getStageResults() {
		return this.results;
	}
}
