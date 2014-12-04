package org.wikidata.wdtk.clt;

import static org.junit.Assert.*;

import org.junit.Test;
import org.wikidata.wdtk.dumpfiles.MwDumpFileMetaData;
import org.wikidata.wdtk.dumpfiles.wmf.WmfDumpFileMetadata;

public class OutputConfigurationTest {

	final static String DATE_STAMP = "20140612";
	final static String PROJECT_NAME = "wikidatawiki";
	final static String OUTPUT_DESTINATION = "instances.nt";
	final static String TEST_FILENAME_WITH_WILDCARDS = "<project_name>-<date_stamp>-instances.nt";
	final static String TEST_FILENAME_WITH_WILDCARDS2 = "\\<project_name\\>-\\<date_stamp\\>-instances.nt";
	final static String TEST_FILENAME = PROJECT_NAME + "-" + DATE_STAMP + "-" + OUTPUT_DESTINATION;
	@Test
	public void testReplaceWildcards() {
		OutputConfiguration outputConfiguration = new JsonConfiguration(new ConversionProperties());
		outputConfiguration.setOutputDestination(TEST_FILENAME_WITH_WILDCARDS);
		MwDumpFileMetaData metaData = new WmfDumpFileMetadata("20140612", "wikidatawiki");
		assertEquals(outputConfiguration.replaceWildcards(metaData), TEST_FILENAME);
		outputConfiguration.setOutputDestination(TEST_FILENAME_WITH_WILDCARDS2);
		assertEquals(outputConfiguration.replaceWildcards(metaData), TEST_FILENAME_WITH_WILDCARDS);
		
	}

}
