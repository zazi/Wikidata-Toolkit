package org.wikidata.wdtk.datamodel.json.jackson;

/*
 * #%L
 * Wikidata Toolkit Data Model
 * %%
 * Copyright (C) 2014 - 2015 Wikidata Toolkit Developers
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author tgaengler
 */

public class ArrayMapDeserializer<MAP_VALUE_CLASS> extends JsonDeserializer<Map<String, MAP_VALUE_CLASS>> {

	@Override
	public Map<String, MAP_VALUE_CLASS> deserialize(final JsonParser jp, final DeserializationContext context) throws IOException {

		final ObjectMapper mapper = (ObjectMapper) jp.getCodec();

		if (jp.getCurrentToken().equals(JsonToken.START_OBJECT)) {

			return mapper.readValue(jp, new TypeReference<HashMap<String, MAP_VALUE_CLASS>>() {
			});
		} else {

			//consume this stream
			mapper.readTree(jp);

			return new HashMap<>();
		}
	}
}
