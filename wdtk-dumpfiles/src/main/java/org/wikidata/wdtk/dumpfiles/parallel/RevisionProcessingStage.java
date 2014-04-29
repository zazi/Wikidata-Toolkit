package org.wikidata.wdtk.dumpfiles.parallel;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import org.json.JSONObject;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.dumpfiles.MwDumpFile;
import org.wikidata.wdtk.dumpfiles.MwDumpFileProcessor;
import org.wikidata.wdtk.dumpfiles.MwDumpFileProcessorImpl;
import org.wikidata.wdtk.dumpfiles.MwRevision;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessor;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessorBroker;
import org.wikidata.wdtk.dumpfiles.StatisticsMwRevisionProcessor;
import org.wikidata.wdtk.dumpfiles.WikibaseRevisionProcessor;

public class RevisionProcessingStage extends
		ContextFreeStage<MwDumpFile, JSONObject> {
	
	// TODO for now this just wraps the rest of the tool chain...

	private MwDumpFileProcessor dfProcessor;
	private MwRevisionProcessorBroker mwRevisionBroker;
	private EntityDocumentProcessor edpItemStats;
	private MwRevisionProcessor rpItemStats;
	private MwRevisionProcessor rpRevisionStats;

	public RevisionProcessingStage() {
		super();

		edpItemStats = new ItemStatisticsProcessor();
		rpItemStats = new WikibaseRevisionProcessor(edpItemStats);
		rpRevisionStats = new StatisticsMwRevisionProcessor(
				"revision processing statistics", 10000);
		mwRevisionBroker = new MwRevisionProcessorBroker();
		dfProcessor = new MwDumpFileProcessorImpl(mwRevisionBroker);

		mwRevisionBroker.registerMwRevisionProcessor(rpItemStats,
				MwRevision.MODEL_WIKIBASE_ITEM, true);
		mwRevisionBroker.registerMwRevisionProcessor(rpRevisionStats, null,
				true);
		this.result = new CounterStageResult();
		this.producers = new LinkedList<>();
	}

	// TODO maybe this stage needs context
	// context is for now provided implicitly by the revision processors

	@Override
	public JSONObject processElement(MwDumpFile element) {
		// TODO find out when the stage should finish
		// for now finishing is done via the master

		try {
			InputStream stream = element.getDumpFileStream();
			// TODO take this apart
			dfProcessor.processDumpFileContents(stream, element);
			((CounterStageResult) this.result).increment();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
	
	// NOTE this could be a stage of its own
	static class ItemStatisticsProcessor implements EntityDocumentProcessor {

		long countItems = 0;
		long countLabels = 0;
		long countDescriptions = 0;
		long countAliases = 0;
		long countStatements = 0;
		long countSiteLinks = 0;

		@Override
		public void processItemDocument(ItemDocument itemDocument) {
			this.countItems++;
			this.countLabels += itemDocument.getLabels().size();
			this.countDescriptions += itemDocument.getDescriptions().size();
			for (String languageKey : itemDocument.getAliases().keySet()) {
				this.countAliases += itemDocument.getAliases().get(languageKey)
						.size();
			}
			for (StatementGroup sg : itemDocument.getStatementGroups()) {
				this.countStatements += sg.getStatements().size();
			}
			this.countSiteLinks += itemDocument.getSiteLinks().size();

			// print a report every 10000 items:
			if (this.countItems % 10000 == 0) {
				printReport();
			}
		}

		@Override
		public void processPropertyDocument(PropertyDocument propertyDocument) {
			// ignore properties
		}

		@Override
		public void finishProcessingEntityDocuments() {
			printReport(); // print a final report
		}

		/**
		 * Prints a report about the statistics gathered so far.
		 */
		private void printReport() {
			System.out.println("Processed " + this.countItems + " items:");
			System.out.println(" * Labels: " + this.countLabels);
			System.out.println(" * Descriptions: " + this.countDescriptions);
			System.out.println(" * Aliases: " + this.countAliases);
			System.out.println(" * Statements: " + this.countStatements);
			System.out.println(" * Site links: " + this.countSiteLinks);
		}

	}

}
