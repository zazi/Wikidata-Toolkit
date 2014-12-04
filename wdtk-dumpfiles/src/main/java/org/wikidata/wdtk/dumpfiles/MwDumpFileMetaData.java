package org.wikidata.wdtk.dumpfiles;

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
