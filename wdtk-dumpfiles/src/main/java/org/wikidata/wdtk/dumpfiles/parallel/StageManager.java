package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
public class StageManager implements Callable<StageResult> {


	/**
	 * The executor that handles running the given threads. A cached thread pool
	 * seems to be the best choice for the requirements of the WDTK.
	 */
	private ExecutorService executor = Executors.newCachedThreadPool();

	/**
	 * The stages to be run by this class instance. Used a set here since it
	 * makes no sense to schedule a given Stage-instance multiple times in one
	 * run.
	 */
	private Set<Stage<?, ?>> scheduledStages = new HashSet<>();
	private Map<Stage<?, ?>, Future<StageResult>> runningStages = new HashMap<>();
	private List<StageResult> results = new LinkedList<>();
	private Future<StageResult> stageManagerFuture;
	private int waitTime = 10000;

	private Logger logger = LoggerFactory.getLogger(StageManager.class);

	public StageManager() {
		this.stageManagerFuture = this.executor.submit(this);
	}

	/**
	 * Runs the StageManagerThread. Internally the thread just waits some time
	 * and then checks if there are Futures that can be collected yet. Once the
	 */
	@Override
	public StageResult call() throws Exception {

		logger.info("Stage manager ready");
		// start out in setup mode, waiting for a signal to run
		// [0]
		synchronized (this) {
			try {
				this.wait();
			} catch (InterruptedException e) {}
		}

		// the working loop
		logger.info("Stage manager running");
		while (!this.runningStages.isEmpty()) {

			synchronized (this) {
				try {
					this.wait(this.waitTime);
				} catch (InterruptedException e) {
				}
			}
			
			this.collectResults();
		}

		this.signalShutdown();
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

		return new NoStageResult();
	}

	/**
	 * Connects two Stages together. The sender stage is the producer of objects
	 * while the receiver stage is the consumer of objects. 
	 * The OutType of the sender must be the same as the InType of the receiver.
	 * Stages that are connected are also automatically submitted.
	 * @param sender
	 * @param receiver
	 * @return
	 */
	public synchronized <CommonType> void connectStages(
			Stage<?, CommonType> sender, Stage<CommonType, ?> receiver) {

		this.submitStage(sender);
		this.submitStage(receiver);

		sender.addConsumer(receiver);
		receiver.addProducer(sender);
	}

	/**
	 * Submits a stage to the set of stages to be launched later.
	 * 
	 * @param toLaunch
	 */
	public synchronized void submitStage(Stage<?, ?> toLaunch) {

		toLaunch.setStageManager(this);
		this.scheduledStages.add(toLaunch);
	}

	/**
	 * Execute all earlier submitted stages.
	 */
	public synchronized void run() {
		
		for (Stage<?, ?> stage : this.scheduledStages) {
			Future<StageResult> result = executor.submit(stage);
			this.runningStages.put(stage, result);
		}
		
		// notify the StageManagers thread to abort the wait for the run-signal
		// see [0]
		this.notify();
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
	 * Signal the shutdown to the executor.
	 */
	protected void signalShutdown() {
		logger.info("Shutting down manager");
		this.executor.shutdown();
	}

	/**
	 * 
	 * @return
	 */
	public StageResult waitForFuture() {
		try {
			return this.stageManagerFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			return new FailedStageResult(e);
		}
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
