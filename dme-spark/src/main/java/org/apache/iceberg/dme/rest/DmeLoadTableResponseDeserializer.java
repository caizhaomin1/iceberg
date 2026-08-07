/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.dme.rest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.dme.vector.VectorDescriptor;
import org.apache.iceberg.dme.vector.VectorDescriptorRegistry;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.credentials.CredentialParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Decodes DME's logical schema from the HTTP response while taking the physical Iceberg schema from
 * the immutable local metadata file identified by {@code metadata-location}.
 */
public final class DmeLoadTableResponseDeserializer extends JsonDeserializer<LoadTableResponse> {
  private static final String METADATA_LOCATION = "metadata-location";
  private static final String METADATA = "metadata";
  private static final String CONFIG = "config";
  private static final String STORAGE_CREDENTIALS = "storage-credentials";

  private final DmeLocalMetadataFileReader metadataReader;

  public DmeLoadTableResponseDeserializer(DmeLocalMetadataFileReader metadataReader) {
    this.metadataReader = metadataReader;
  }

  @Override
  public LoadTableResponse deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {
    JsonNode response = parser.getCodec().readTree(parser);
    JsonNode locationNode = response.get(METADATA_LOCATION);
    Preconditions.checkArgument(
        locationNode != null && locationNode.isTextual(),
        "DME response is missing metadata-location");

    String metadataLocation = locationNode.asText();
    TableMetadata metadata = metadataReader.read(metadataLocation);
    registerVectorDescriptors(response.get(METADATA), metadata);

    LoadTableResponse.Builder builder =
        LoadTableResponse.builder()
            .withTableMetadata(
                TableMetadata.buildFrom(metadata).withMetadataLocation(metadataLocation).build());

    JsonNode config = response.get(CONFIG);
    if (config != null && config.isObject()) {
      Iterator<Entry<String, JsonNode>> fields = config.fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> entry = fields.next();
        if (entry.getValue().isTextual()) {
          builder.addConfig(entry.getKey(), entry.getValue().asText());
        }
      }
    }

    JsonNode credentials = response.get(STORAGE_CREDENTIALS);
    if (credentials != null && credentials.isArray()) {
      for (JsonNode credential : credentials) {
        builder.addCredential(CredentialParser.fromJson(credential));
      }
    }
    return builder.build();
  }

  private static void registerVectorDescriptors(JsonNode logicalMetadata, TableMetadata physical) {
    if (logicalMetadata == null || !logicalMetadata.isObject()) {
      return;
    }

    JsonNode schemas = logicalMetadata.get("schemas");
    if (schemas == null || !schemas.isArray()) {
      return;
    }

    int currentSchemaId =
        logicalMetadata.path("current-schema-id").asInt(physical.currentSchemaId());
    JsonNode logicalSchema = null;
    for (JsonNode candidate : schemas) {
      if (candidate.path("schema-id").asInt(Integer.MIN_VALUE) == currentSchemaId) {
        logicalSchema = candidate;
        break;
      }
    }
    if (logicalSchema == null) {
      return;
    }

    List<VectorDescriptor> descriptors = new ArrayList<>();
    Schema physicalSchema = physical.schema();
    JsonNode fields = logicalSchema.get("fields");
    if (fields != null && fields.isArray()) {
      for (JsonNode logicalField : fields) {
        if ("floatvector".equals(logicalField.path("type").asText())) {
          descriptors.add(toDescriptor(logicalField, physicalSchema));
        }
      }
    }
    VectorDescriptorRegistry.get().replace(physical.uuid(), descriptors);
  }

  private static VectorDescriptor toDescriptor(JsonNode logicalField, Schema physicalSchema) {
    int dimension = logicalField.path("dim").asInt(Integer.MIN_VALUE);
    String name = logicalField.path("name").asText();
    Preconditions.checkArgument(
        !name.isBlank(), "Invalid DME floatvector field name: %s", logicalField);
    Preconditions.checkArgument(
        dimension > 0, "Invalid DME floatvector dimension: %s", logicalField);

    // DME assigns new field IDs itself. The name is the stable correspondence at this boundary;
    // subsequent Spark operations use the actual field ID obtained from metadata.json.
    Types.NestedField physicalField = physicalSchema.findField(name);
    if (physicalField == null) {
      throw new RESTException("DME floatvector field is absent from metadata.json: name=%s", name);
    }
    Type fieldType = physicalField.type();
    if (!fieldType.isListType()) {
      throw new RESTException("DME floatvector field must be list<float>: name=%s", name);
    }
    Types.ListType list = fieldType.asListType();
    if (list.elementType().typeId() != Type.TypeID.FLOAT || !list.isElementOptional()) {
      throw new RESTException(
          "DME floatvector physical type must be list<optional float>: name=%s", name);
    }

    return new VectorDescriptor(
        physicalField.fieldId(), name, dimension, list.elementId(), !physicalField.isOptional());
  }
}
