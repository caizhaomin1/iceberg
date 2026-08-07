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
package org.apache.iceberg.dme.vector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.dme.rest.DmeVectorJsonCodec;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;

/** Executor-side validation for DME FLOATVECTOR values. */
public final class DmeVectorValidator implements Serializable {
  private final List<Rule> rules;

  private DmeVectorValidator(List<Rule> rules) {
    this.rules = rules;
  }

  public static DmeVectorValidator forTable(Schema schema, String tableUuid) {
    return forSchema(schema, tableUuid, Map.of());
  }

  /** Also supports uncommitted staged creates, before DME has returned a logical load response. */
  public static DmeVectorValidator forTable(Table table) {
    return forSchema(table.schema(), table.uuid().toString(), declarations(table.properties()));
  }

  private static DmeVectorValidator forSchema(
      Schema schema, String tableUuid, Map<String, Integer> stagedDeclarations) {
    List<Rule> rules = new ArrayList<>();
    for (int ordinal = 0; ordinal < schema.columns().size(); ordinal += 1) {
      int fieldOrdinal = ordinal;
      Types.NestedField field = schema.columns().get(ordinal);
      VectorDescriptorRegistry.get()
          .find(tableUuid, field.fieldId())
          .ifPresentOrElse(
              descriptor ->
                  rules.add(
                      new Rule(
                          fieldOrdinal,
                          descriptor.name(),
                          descriptor.dimension(),
                          descriptor.required())),
              () -> {
                Integer dimension = stagedDeclarations.get(field.name());
                if (dimension != null) {
                  rules.add(new Rule(fieldOrdinal, field.name(), dimension, field.isRequired()));
                }
              });
    }
    return new DmeVectorValidator(rules);
  }

  private static Map<String, Integer> declarations(Map<String, String> properties) {
    String value = properties.get(DmeVectorJsonCodec.DECLARATIONS_PROPERTY);
    if (value == null || value.isBlank()) {
      return Map.of();
    }
    java.util.Map<String, Integer> declarations = new java.util.HashMap<>();
    for (String declaration : Splitter.on(',').split(value)) {
      List<String> parts = Splitter.on(':').splitToList(declaration);
      if (parts.size() == 2) {
        declarations.put(parts.get(0), Integer.parseInt(parts.get(1)));
      }
    }
    return declarations;
  }

  public boolean hasVectors() {
    return !rules.isEmpty();
  }

  public void validate(InternalRow row) {
    for (Rule rule : rules) {
      if (row.isNullAt(rule.ordinal)) {
        if (rule.required) {
          throw new IllegalArgumentException(
              "DME_FLOATVECTOR_NULL: column=" + rule.name + " does not allow null values");
        }
        continue;
      }

      ArrayData values = row.getArray(rule.ordinal);
      if (values.numElements() != rule.dimension) {
        throw new IllegalArgumentException(
            "DME_FLOATVECTOR_DIM_MISMATCH: column="
                + rule.name
                + ", expected="
                + rule.dimension
                + ", actual="
                + values.numElements());
      }

      for (int index = 0; index < values.numElements(); index += 1) {
        if (!values.isNullAt(index) && !Float.isFinite(values.getFloat(index))) {
          throw new IllegalArgumentException(
              "DME_FLOATVECTOR_INVALID_FLOAT: column="
                  + rule.name
                  + ", index="
                  + index
                  + ", NaN and infinity are not supported");
        }
      }
    }
  }

  private static final class Rule implements Serializable {
    private final int ordinal;
    private final String name;
    private final int dimension;
    private final boolean required;

    private Rule(int ordinal, String name, int dimension, boolean required) {
      this.ordinal = ordinal;
      this.name = name;
      this.dimension = dimension;
      this.required = required;
    }
  }
}
