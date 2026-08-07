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
package org.apache.iceberg.dme.spark;

import org.apache.iceberg.Table;
import org.apache.iceberg.dme.vector.DmeVectorValidator;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

/** Spark table wrapper that adds executor-side FLOATVECTOR validation. */
public class DmeSparkTable extends SparkTable {
  private final boolean refreshEagerly;

  public DmeSparkTable(Table table, boolean refreshEagerly) {
    super(table, refreshEagerly);
    this.refreshEagerly = refreshEagerly;
  }

  public DmeSparkTable(Table table, long snapshotId, boolean refreshEagerly) {
    super(table, snapshotId, refreshEagerly);
    this.refreshEagerly = refreshEagerly;
  }

  public DmeSparkTable(Table table, String branch, boolean refreshEagerly) {
    super(table, branch, refreshEagerly);
    this.refreshEagerly = refreshEagerly;
  }

  @Override
  public SparkTable copyWithSnapshotId(long newSnapshotId) {
    return new DmeSparkTable(table(), newSnapshotId, refreshEagerly);
  }

  @Override
  public SparkTable copyWithBranch(String targetBranch) {
    return new DmeSparkTable(table(), targetBranch, refreshEagerly);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    WriteBuilder delegate = super.newWriteBuilder(info);
    DmeVectorValidator validator = DmeVectorValidator.forTable(table());
    return validator.hasVectors() ? new DmeValidatingWriteBuilder(delegate, validator) : delegate;
  }
}
