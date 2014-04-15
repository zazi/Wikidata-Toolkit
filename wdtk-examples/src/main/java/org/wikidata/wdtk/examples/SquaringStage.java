package org.wikidata.wdtk.examples;

import java.util.LinkedList;

import org.wikidata.wdtk.dumpfiles.parallel.ContextFreeStage;
import org.wikidata.wdtk.dumpfiles.parallel.CounterStageResult;

public class SquaringStage extends ContextFreeStage<Integer, Integer> {

	SquaringStage(){
		this.result = new CounterStageResult();
		this.producers = new LinkedList<>();
		this.consumers = new LinkedList<>();
	}
	
	@Override
	public Integer processElement(Integer element) {
		
		((CounterStageResult) this.result).increment();
		return element * element;
	}

}
