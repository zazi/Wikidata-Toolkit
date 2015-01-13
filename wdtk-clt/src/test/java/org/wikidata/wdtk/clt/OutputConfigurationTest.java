package org.wikidata.wdtk.clt;

/*
 * #%L
 * Wikidata Toolkit Command-line Tool
 * %%
 * Copyright (C) 2014 Wikidata Toolkit Developers
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
	@Test
	public void testValidateExcluders(){
		OutputConfiguration outputConfiguration = new JsonConfiguration(new ConversionProperties());
		assertTrue(outputConfiguration.validateExcluders(TEST_FILENAME_WITH_WILDCARDS));
		assertTrue(outputConfiguration.validateExcluders(TEST_FILENAME_WITH_WILDCARDS2));
		assertTrue(!outputConfiguration.validateExcluders(TEST_FILENAME_WITH_WILDCARDS2 + "\\"));
	}

}
