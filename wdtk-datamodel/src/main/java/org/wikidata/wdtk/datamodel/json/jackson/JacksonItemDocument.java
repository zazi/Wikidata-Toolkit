package org.wikidata.wdtk.datamodel.json.jackson;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelConverter;
import org.wikidata.wdtk.datamodel.helpers.Equality;
import org.wikidata.wdtk.datamodel.helpers.Hash;
import org.wikidata.wdtk.datamodel.helpers.ToString;
import org.wikidata.wdtk.datamodel.implementation.ItemDocumentImpl;
import org.wikidata.wdtk.datamodel.implementation.SiteLinkImpl;
import org.wikidata.wdtk.datamodel.implementation.StatementImpl;
import org.wikidata.wdtk.datamodel.interfaces.EntityIdValue;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyIdValue;
import org.wikidata.wdtk.datamodel.interfaces.SiteLink;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;

/**
 * Jackson implementation of {@link ItemDocument}. Like all Jackson objects, it
 * is not technically immutable, but it is strongly recommended to treat it as
 * such in all contexts: the setters are for Jackson; never call them in your
 * code.
 *
 * @author Fredo Erxleben
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JacksonItemDocument extends JacksonTermedStatementDocument
		implements ItemDocument {

	/**
	 * Map to store site links.
	 */
	@JsonDeserialize(using = JacksonSiteLinkArrayMapDeserializer.class)
	private Map<String, JacksonSiteLink> sitelinks = new HashMap<>();

	/**
	 * Constructor. Creates an empty object that can be populated during JSON
	 * deserialization. Should only be used by Jackson for this very purpose.
	 */
	public JacksonItemDocument() {
	}

	@JsonIgnore
	@Override
	public ItemIdValue getItemId() {
		if (this.siteIri == null) {
			return Datamodel.makeWikidataItemIdValue(this.entityId);
		} else {
			return Datamodel.makeItemIdValue(this.entityId, this.siteIri);
		}
	}

	@JsonIgnore
	@Override
	public EntityIdValue getEntityId() {
		return getItemId();
	}

	/**
	 * Sets the site links to the given value. Only for use by Jackson during
	 * deserialization.
	 *
	 * @param sitelinks
	 *            new value
	 */
	@JsonProperty("sitelinks")
	public void setSiteLinks(Map<String, JacksonSiteLink> sitelinks) {
		this.sitelinks = sitelinks;
	}

	@JsonProperty("sitelinks")
	@Override
	public Map<String, SiteLink> getSiteLinks() {
		return Collections.<String, SiteLink>unmodifiableMap(this.sitelinks);
	}

	@Override
	public int hashCode() {
		return Hash.hashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return Equality.equalsItemDocument(this, obj);
	}

	@Override
	public String toString() {
		return ToString.toString(this);
	}

	public static JacksonItemDocument fromItemDocumentImpl(final ItemDocumentImpl itemDocument) {

		final JacksonItemDocument jacksonItemDocument = new JacksonItemDocument();
		jacksonItemDocument.setJsonId(null);
		jacksonItemDocument.setLabels(transformMonolingualTextValues(itemDocument.getLabels()));
		jacksonItemDocument.setDescriptions(transformMonolingualTextValues(itemDocument.getDescriptions()));
		jacksonItemDocument.setAliases(transformMonolingualTextValueMaps(itemDocument.getAliases()));
		jacksonItemDocument.setJsonClaims(transformClaims(itemDocument.getStatementGroups()));
		jacksonItemDocument.setSiteLinks(transformSiteLinks(itemDocument.getSiteLinks()));

		return jacksonItemDocument;
	}

	private static Map<String, List<JacksonMonolingualTextValue>> transformMonolingualTextValueMaps(
			final Map<String, List<MonolingualTextValue>> monolingualTextValueMaps) {

		if (monolingualTextValueMaps == null) {

			return null;
		}

		final Map<String, List<JacksonMonolingualTextValue>> jacksonMonolingualTextValues = new HashMap<>();

		for (final Map.Entry<String, List<MonolingualTextValue>> monolingualTextValueEntry : monolingualTextValueMaps.entrySet()) {

			final String monolingualTextValueKey = monolingualTextValueEntry.getKey();
			final List<MonolingualTextValue> monolingualTextValues = monolingualTextValueEntry.getValue();

			final List<JacksonMonolingualTextValue> jacksonMonolingualTextValueList = new ArrayList<>();

			for (final MonolingualTextValue monolingualTextValue : monolingualTextValues) {

				final JacksonMonolingualTextValue jacksonMonolingualTextValue = new JacksonMonolingualTextValue(monolingualTextValue);

				jacksonMonolingualTextValueList.add(jacksonMonolingualTextValue);
			}

			jacksonMonolingualTextValues.put(monolingualTextValueKey, jacksonMonolingualTextValueList);
		}

		return jacksonMonolingualTextValues;
	}

	private static Map<String, List<JacksonStatement>> transformClaims(final List<StatementGroup> statementGroups) {

		if (statementGroups == null) {

			return null;
		}

		// TODO: are claims grouped by subject or property?
		final Map<String, List<JacksonStatement>> jacksonClaims = new HashMap<>();

		for (final StatementGroup statementGroup : statementGroups) {

			final PropertyIdValue property = statementGroup.getProperty();

			final List<Statement> statements = statementGroup.getStatements();

			final List<JacksonStatement> jacksonStatements = new ArrayList<>();

			if (statements != null) {

				for (final Statement statement : statements) {

					final JacksonStatement jacksonStatement = JacksonStatement.fromStatementImpl((StatementImpl) statement);

					jacksonStatements.add(jacksonStatement);
				}
			}

			// TODO id or iri
			jacksonClaims.put(property.getId(), jacksonStatements);
		}

		return jacksonClaims;
	}

	private static Map<String, JacksonSiteLink> transformSiteLinks(final Map<String, SiteLink> siteLinks) {

		if (siteLinks == null) {

			return null;
		}

		final Map<String, JacksonSiteLink> jacksonSiteLinks = new HashMap<>();

		for (final Map.Entry<String, SiteLink> siteLinkEntry : siteLinks.entrySet()) {

			final String siteLinkKey = siteLinkEntry.getKey();
			final SiteLink siteLink = siteLinkEntry.getValue();

			final JacksonSiteLink jacksonSiteLink = JacksonSiteLink.fromSiteLinkImpl((SiteLinkImpl) siteLink);

			jacksonSiteLinks.put(siteLinkKey, jacksonSiteLink);
		}

		return jacksonSiteLinks;
	}
}
