package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the life cycle of all processing stages and the
 * connections between these stages.
 * 
 * @author Fredo Erxleben
 * 
 */
public class StageManager implements Callable<String> {
	// XXX the generic type of the Callable-Interface is not fixed
	// See what will be better fitting…

	// TODO the manager shut down, once all stages are done, but should be able
	// to be restarted later again via run()

	private ExecutorService executor = Executors.newCachedThreadPool();
	private List<Stage<?, ?>> scheduledStages = new LinkedList<>();
	private Map<Stage<?, ?>, Future<StageResult>> runningStages = new HashMap<>();
	private List<StageResult> results = new LinkedList<>();
	private Future<String> stageManagerFuture;
	private boolean running = false;

	private Logger logger = LoggerFactory.getLogger(StageManager.class);

	public StageManager() {
		this.stageManagerFuture = this.executor.submit(this);
	}

	/**
	 * Runs the StageManagerThread. Internally the thread just waits some time
	 * and then checks if there are Futures that can be collected yet. Once the
	 */
	@Override
	public String call() throws Exception {

		logger.info("Stage manager running");
		// start out in setup mode, waiting for a signal to run
		synchronized (this) {
			try {
				this.wait();
			} catch (InterruptedException e) {
			}
		}

		// the working loop
		while (this.running) {

			this.collectResults();

			synchronized (this) {
				try {
					// TODO move wait time into a field
					this.wait(10000);
				} catch (InterruptedException e) {
				}
			}
		}

		this.scheduledStages.clear();

		// get all the outstanding results
		// before quitting
		for (Future<StageResult> future : this.runningStages.values()) {
			try {
				this.results.add(future.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		this.runningStages.clear();

		return null;
	}

	/**
	 * 
	 * @param sender
	 * @param receiver
	 * @return
	 */
	public synchronized <CommonType> void connectStages(
			Stage<?, CommonType> sender, Stage<CommonType, ?> receiver) {

		sender.addConsumer(receiver);
		receiver.addProducer(sender);
	}

	/**
	 * Submits a stage to the set of stages to be launched later.
	 * 
	 * @param toLaunch
	 */
	public synchronized void submitStage(Stage<?, ?> toLaunch) {

		this.scheduledStages.add(toLaunch);
	}

	/**
	 * Executes all earlier submitted stages. This hands the stages threads over
	 * to the
	 */
	public synchronized void run() {
		for (Stage<?, ?> stage : this.scheduledStages) {
			Future<StageResult> result = executor.submit(stage);
			this.runningStages.put(stage, result);
		}

		if (!this.running) {
			this.notify();
		}
		this.running = true;
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
					logger.info("Collecting Result for Stage "
							+ future.getKey().toString());
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
	 * The method will return, once all Futures are collected
	 */
	public void signalShutdown() {
		logger.info("Shutting down manager");
		this.executor.shutdown();
		this.running = false;
	}

	public String waitForFuture() {
		try {
			return this.stageManagerFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return null;
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
