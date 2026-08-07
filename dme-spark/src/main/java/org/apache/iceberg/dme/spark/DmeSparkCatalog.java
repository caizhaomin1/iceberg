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

import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.dme.rest.DmeRESTCatalog;
import org.apache.iceberg.dme.vector.DmeVectorDeclarationContext;
import org.apache.iceberg.dme.vector.VectorDescriptorRegistry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 catalog adapter for the DME REST catalog implementation. */
public class DmeSparkCatalog extends SparkCatalog {
  @Override
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    SparkSession spark = SparkSession.active();
    String extensions = spark.conf().get("spark.sql.extensions", "");
    Preconditions.checkArgument(
        extensions.contains(DmeSparkExtensions.class.getName()),
        "DME Runtime requires spark.sql.extensions to include %s",
        DmeSparkExtensions.class.getName());

    String configuredCatalogImpl = options.get(CatalogProperties.CATALOG_IMPL);
    if (configuredCatalogImpl != null
        && !configuredCatalogImpl.equals(DmeRESTCatalog.class.getName())) {
      throw new IllegalArgumentException(
          "DME Runtime always uses "
              + DmeRESTCatalog.class.getName()
              + "; do not configure "
              + CatalogProperties.CATALOG_IMPL);
    }

    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(spark, name);
    Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    properties.putAll(options.asCaseSensitiveMap());
    properties.remove(CatalogProperties.CATALOG_IMPL);
    properties.put(CatalogProperties.APP_ID, spark.sparkContext().applicationId());
    properties.put(CatalogProperties.USER, spark.sparkContext().sparkUser());

    DmeRESTCatalog catalog = new DmeRESTCatalog();
    catalog.setConf(conf);
    catalog.initialize(name, properties);
    return catalog;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Table loaded = super.loadTable(ident);
    if (loaded instanceof org.apache.iceberg.spark.source.SparkTable) {
      org.apache.iceberg.spark.source.SparkTable sparkTable =
          (org.apache.iceberg.spark.source.SparkTable) loaded;
      return new DmeSparkTable(sparkTable.table(), true);
    }
    return loaded;
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Table created = super.createTable(ident, schema, transforms, properties);
    org.apache.iceberg.spark.source.SparkTable sparkTable =
        (org.apache.iceberg.spark.source.SparkTable) created;
    return new DmeSparkTable(sparkTable.table(), true);
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    try {
      return new DmeStagedSparkTable(
          newCreateBuilder(ident, schema, transforms, properties).createTransaction());
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws NoSuchTableException {
    try {
      return new DmeStagedSparkTable(
          newCreateBuilder(ident, schema, transforms, properties).replaceTransaction());
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    return new DmeStagedSparkTable(
        newCreateBuilder(ident, schema, transforms, properties).createOrReplaceTransaction());
  }

  @Override
  public Table alterTable(
      Identifier ident, org.apache.spark.sql.connector.catalog.TableChange... changes)
      throws NoSuchTableException {
    try {
      Table altered = super.alterTable(ident, changes);
      org.apache.iceberg.spark.source.SparkTable sparkTable =
          (org.apache.iceberg.spark.source.SparkTable) altered;
      return new DmeSparkTable(sparkTable.table(), true);
    } finally {
      DmeVectorDeclarationContext.clear();
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      org.apache.iceberg.Table table = icebergCatalog().loadTable(buildIdentifier(ident));
      VectorDescriptorRegistry.get().remove(table.uuid().toString());
    } catch (org.apache.iceberg.exceptions.NoSuchTableException ignored) {
      // SparkCatalog will return false for the same condition.
    }
    return super.dropTable(ident);
  }

  private Catalog.TableBuilder newCreateBuilder(
      Identifier ident,
      StructType sparkSchema,
      Transform[] transforms,
      Map<String, String> properties) {
    Schema schema = SparkSchemaUtil.convert(sparkSchema);
    return icebergCatalog()
        .buildTable(buildIdentifier(ident), schema)
        .withPartitionSpec(Spark3Util.toPartitionSpec(schema, transforms))
        .withLocation(properties.get("location"))
        .withProperties(Spark3Util.rebuildCreateProperties(properties));
  }
}
