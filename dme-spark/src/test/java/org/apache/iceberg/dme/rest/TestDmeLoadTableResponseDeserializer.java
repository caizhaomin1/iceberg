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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.dme.vector.VectorDescriptor;
import org.apache.iceberg.dme.vector.VectorDescriptorRegistry;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDmeLoadTableResponseDeserializer {
  @TempDir private Path temp;
  private String tableUuid;

  @AfterEach
  void clearRegistry() {
    if (tableUuid != null) {
      VectorDescriptorRegistry.get().remove(tableUuid);
    }
  }

  @Test
  void getsActualElementIdFromMetadataLocation() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                22, "vec_col", Types.ListType.ofOptional(107, Types.FloatType.get())));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            temp.resolve("table").toString(),
            java.util.Map.of());
    Path metadataPath = temp.resolve("v1.metadata.json");
    TableMetadataParser.write(metadata, Files.localOutput(metadataPath.toString()));

    String json =
        "{\"metadata-location\":\""
            + metadataPath
            + "\",\"metadata\":{\"current-schema-id\":0,\"schemas\":[{\"schema-id\":0,\"fields\":[{\"id\":22,\"name\":\"vec_col\",\"required\":false,\"type\":\"floatvector\",\"dim\":1024}]}]}}";
    LoadTableResponse response =
        DmeRestObjectMapper.create().readValue(json, LoadTableResponse.class);
    tableUuid = response.tableMetadata().uuid();

    int actualFieldId = response.tableMetadata().schema().findField("vec_col").fieldId();
    VectorDescriptor descriptor =
        VectorDescriptorRegistry.get().find(tableUuid, actualFieldId).orElseThrow();
    assertThat(descriptor.dimension()).isEqualTo(1024);
    assertThat(descriptor.elementId())
        .isEqualTo(
            response.tableMetadata().schema().findField("vec_col").type().asListType().elementId());
  }
}
