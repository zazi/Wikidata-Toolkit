package org.wikidata.wdtk.dumpfiles.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * An ElementProcessingStage is a Stage that processes the input given by the
 * producers element-wise without respect to other elements. The elements are
 * processed in the order the producer makes them available. If there are more
 * then one producer, the input queues might be interleaved.
 * 
 * If you need contextual information about other elements (e.g. for sorting)
 * you will have to do your own implementation.
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
	 * is available.
	 */
	protected void afterStep() {
		synchronized (this) {
			try {
				wait(this.waitTime);
			} catch (InterruptedException e) {
				// e.printStackTrace();
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
			this.afterStep();

		}
		this.finished = true;
		return this.result;
	}

}
