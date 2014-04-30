package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.LinkedList;
import java.util.List;

/**
 * An ContextFreeStage is a Stage that processes the input given by the
 * producers element-wise without respect to other elements. The elements are
 * processed in the order the producers makes them available in the input queue.
 * 
 * Once the producers of this stage all have finished, the stage will process
 * the remaining elements and then finish too.
 * 
 * If you need contextual information about other elements (e.g. for sorting)
 * you will have to use another implementation (or do your own).
 * 
 * @author Fredo Erxleben
 * 
 * @param <InType>
 *            the type of the incoming elements to be processed
 * @param <OutType>
 *            the type of the outgoing elements of each processing step
 */
public abstract class ContextFreeStage<InType, OutType> extends
		Stage<InType, OutType> {

	/**
	 * What happens after each step. Default implementation is waiting for more
	 * input. The wait might be aborted and the InterruptedException is silently
	 * caught. This is so you can abort the waiting via notify() once new input
	 * is available. The wait time is limited, so the thread does not wait
	 * indefinitely given the case that all producers have finished without
	 * producing any more output.
	 */
	protected void afterStep() {
		synchronized (this) {
			if (this.inputQueue.isEmpty()) {
				try {
					this.wait(this.waitTime);
				} catch (InterruptedException e) {}
			}
		}
	}

	/**
	 * The default implementation processes the input only element-wise
	 */
	@Override
	public StageResult call() throws Exception {
		List<InType> currentStep = new LinkedList<>();

		while (this.running) {

			// check if there are any more alive producers
			// otherwise finish
			this.running = false;
			for (Stage<?, InType> s : this.producers) {
				if (!s.isFinished()) {
					this.running = true;
					break;
				}
			}

			// get all the input for the steps
			this.inputQueue.drainTo(currentStep);
			// process the elements
			while (!currentStep.isEmpty()) {
				OutType stepResult = this.processElement(currentStep.remove(0));
				// distribute result to all consumers
				this.distribute(stepResult);
			}

			// wait for new input
			this.afterStep();

		}
		this.finished = true;
		this.notifyStageManager();
		return this.result;
	}

}
