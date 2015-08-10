package org.wikidata.wdtk.datamodel.json.jackson.datavalues;

import org.wikidata.wdtk.datamodel.helpers.Equality;
import org.wikidata.wdtk.datamodel.helpers.Hash;
import org.wikidata.wdtk.datamodel.helpers.ToString;
import org.wikidata.wdtk.datamodel.implementation.ItemIdValueImpl;
import org.wikidata.wdtk.datamodel.interfaces.EntityIdValue;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.ValueVisitor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonDeserializer.None;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/*
 * #%L
 * Wikidata Toolkit Data Model
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

/**
 * Jackson implementation of {@link ItemIdValue}.
 *
 * @author Fredo Erxleben
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(using = None.class)
public class JacksonValueItemId extends JacksonValueEntityId implements
		ItemIdValue {

	/**
	 * Sets the inner value helper object to the given value. Only for use by
	 * Jackson during deserialization.
	 *
	 * @param value
	 *            new value
	 */
	@Override
	public void setValue(JacksonInnerEntityId value) {
		if (!JacksonInnerEntityId.JSON_ENTITY_TYPE_ITEM.equals(value
				.getJsonEntityType())) {
			throw new RuntimeException("Unexpected inner value type: "
					+ value.getJsonEntityType());
		}
		this.value = value;
	}

	@JsonIgnore
	@Override
	public String getEntityType() {
		return EntityIdValue.ET_ITEM;
	}

	@Override
	public <T> T accept(ValueVisitor<T> valueVisitor) {
		return valueVisitor.visit(this);
	}

	@Override
	public int hashCode() {
		return Hash.hashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return Equality.equalsEntityIdValue(this, obj);
	}

	@Override
	public String toString() {
		return ToString.toString(this);
	}

	public static JacksonValueItemId fromItemIdValueImpl(final ItemIdValueImpl itemIdValue) {

		final JacksonValueItemId jacksonValueItemId = new JacksonValueItemId();

		final String itemIdString = itemIdValue.getId();
		// remove item pr property prefix
		final String itemIdSubstring = itemIdString.substring(1, itemIdString.length());

		final String entityType = JacksonInnerEntityId.JSON_ENTITY_TYPE_ITEM;

		final JacksonInnerEntityId jacksonInnerEntityId = new JacksonInnerEntityId(entityType, Integer.valueOf(itemIdSubstring));

		jacksonValueItemId.setValue(jacksonInnerEntityId);
		jacksonValueItemId.setSiteIri(itemIdValue.getSiteIri());

		return jacksonValueItemId;
	}
}
