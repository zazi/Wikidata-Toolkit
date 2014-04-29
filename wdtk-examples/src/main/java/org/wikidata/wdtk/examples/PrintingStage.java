package org.wikidata.wdtk.examples;

import org.wikidata.wdtk.dumpfiles.parallel.ContextFreeStage;
import org.wikidata.wdtk.dumpfiles.parallel.CounterStageResult;

public class PrintingStage<InType> extends ContextFreeStage<InType, Void> {
	
	PrintingStage(){
		this.result = new CounterStageResult();
	}
	
	@Override
	public Void processElement(InType element) {
		
		((CounterStageResult) this.result).increment();
		System.out.println(element.toString());
		return null;
	}

}
