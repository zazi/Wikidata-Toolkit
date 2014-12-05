package org.wikidata.wdtk.dumpfiles;

/*
 * #%L
 * Wikidata Toolkit Dump File Handling
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

public interface MwDumpFileMetaData {
	/**
	 * Returns the project name for this dump. Together with the dump content
	 * type and date stamp, this identifies the dump, and it is therefore always
	 * available.
	 * 
	 * @return a project name string
	 */
	String getProjectName();

	/**
	 * Returns the date stamp for this dump. Together with the project name and
	 * dump content type, this identifies the dump, and it is therefore always
	 * available.
	 * 
	 * @return a string that represents a date in format YYYYMMDD
	 */
	String getDateStamp();
}
