package org.wikidata.wdtk.storage.endpoint.shared;

import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.implementation.DataObjectFactoryImpl;
import org.wikidata.wdtk.datamodel.implementation.ItemDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;

public class WdtkItemQueryResult implements WdtkQueryResult {
	
	private static final long serialVersionUID = 8226520115013917870L;

	static final DatamodelConverter converter = new DatamodelConverter(
			new DataObjectFactoryImpl());

	private ItemDocumentImpl resultDocument;

	public WdtkItemQueryResult(ItemDocument resultDocument) {
		ItemDocumentImpl temp = (ItemDocumentImpl) converter
				.deepCopy(resultDocument);
		this.resultDocument = temp;
	}

	public ItemDocumentImpl getResultDocument() {
		return this.resultDocument;
	}
	
	@Override
	public String toString(){
		return this.resultDocument.toString();
	}
}
