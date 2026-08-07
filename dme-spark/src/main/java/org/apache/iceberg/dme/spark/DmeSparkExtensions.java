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

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

/** Spark SQL extension that recognizes {@code FLOATVECTOR(dimension)} in DME DDL. */
public final class DmeSparkExtensions
    implements scala.Function1<SparkSessionExtensions, BoxedUnit> {
  @Override
  public BoxedUnit apply(SparkSessionExtensions extensions) {
    // This runtime is the only extension users configure; retain all standard Iceberg SQL rules.
    new IcebergSparkSessionExtensions().apply(extensions);
    extensions.injectParser(
        new AbstractFunction2<SparkSession, ParserInterface, ParserInterface>() {
          @Override
          public ParserInterface apply(SparkSession session, ParserInterface parser) {
            return new DmeSparkSqlParser(parser);
          }
        });
    return BoxedUnit.UNIT;
  }
}
