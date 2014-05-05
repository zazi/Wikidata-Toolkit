package org.wikidata.wdtk.examples.parallelized;

import org.wikidata.wdtk.dumpfiles.parallel.ContextFreeStage;
import org.wikidata.wdtk.dumpfiles.parallel.CounterStageResult;

public class SquaringStage extends ContextFreeStage<Integer, Integer> {

	SquaringStage(){
		this.result = new CounterStageResult();
	}
	
	@Override
	public Integer processElement(Integer element) {
		
		((CounterStageResult) this.result).increment();
		return element * element;
	}

}
