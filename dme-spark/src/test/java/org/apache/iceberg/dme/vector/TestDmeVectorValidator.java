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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestDmeVectorValidator {
  private static final String TABLE_UUID = "test-vector-table";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(
              22, "vec_col", Types.ListType.ofOptional(23, Types.FloatType.get())));

  @AfterEach
  void clearRegistry() {
    VectorDescriptorRegistry.get().remove(TABLE_UUID);
  }

  @Test
  void rejectsWrongDimensionAndNonFiniteValues() {
    VectorDescriptorRegistry.get()
        .replace(TABLE_UUID, List.of(new VectorDescriptor(22, "vec_col", 3, 23, false)));
    DmeVectorValidator validator = DmeVectorValidator.forTable(SCHEMA, TABLE_UUID);

    assertThatThrownBy(
            () ->
                validator.validate(
                    new GenericInternalRow(
                        new Object[] {new GenericArrayData(new float[] {1F, 2F})})))
        .hasMessageContaining("DME_FLOATVECTOR_DIM_MISMATCH");
    assertThatThrownBy(
            () ->
                validator.validate(
                    new GenericInternalRow(
                        new Object[] {new GenericArrayData(new float[] {1F, Float.NaN, 3F})})))
        .hasMessageContaining("DME_FLOATVECTOR_INVALID_FLOAT");
  }
}
