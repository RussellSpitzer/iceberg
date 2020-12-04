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

package org.apache.iceberg.actions;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;

abstract class Spark3CreateAction implements CreateAction {
  private static final Set<String> ALLOWED_SOURCES = ImmutableSet.of("parquet", "avro", "orc", "hive");
  protected static final String ICEBERG_METADATA_FOLDER = "metadata";

  private final SparkSession spark;

  // Source Fields
  private final V1Table sourceTable;
  private final CatalogTable sourceCatalogTable;
  private final String sourceTableLocation;
  private final CatalogPlugin sourceCatalog;
  private final Identifier sourceTableIdent;

  // Destination Fields
  private final StagingTableCatalog destCatalog;
  private final Identifier destTableIdent;

  // Optional Parameters for destination
  private Map<String, String> additionalProperties = Maps.newHashMap();

  Spark3CreateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent,
                       CatalogPlugin destCatalog, Identifier destTableIdent) {

    this.spark = spark;
    this.sourceCatalog = checkSourceCatalog(sourceCatalog);
    this.sourceTableIdent = sourceTableIdent;
    this.destCatalog = checkDestinationCatalog(destCatalog);
    this.destTableIdent = destTableIdent;

    try {
      this.sourceTable = (V1Table) ((TableCatalog) sourceCatalog).loadTable(sourceTableIdent);
      this.sourceCatalogTable = sourceTable.v1Table();
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException(String.format("Cannot not find source table %s", sourceTableIdent), e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(String.format("Cannot use non-v1 table %s as a source", sourceTableIdent), e);
    }
    validateSourceTable(sourceCatalogTable);

    this.sourceTableLocation = CatalogUtils.URIToString(sourceCatalogTable.storage().locationUri().get());
  }

  @Override
  public CreateAction withProperties(Map<String, String> properties) {
    this.additionalProperties.putAll(properties);
    return this;
  }

  @Override
  public CreateAction withProperty(String key, String value) {
    this.additionalProperties.put(key, value);
    return this;
  }

  protected SparkSession spark() {
    return spark;
  }

  protected String sourceTableLocation() {
    return sourceTableLocation;
  }

  protected CatalogTable v1SourceTable() {
    return sourceCatalogTable;
  }

  protected CatalogPlugin sourceCatalog() {
    return sourceCatalog;
  }

  protected Identifier sourceTableName() {
    return sourceTableIdent;
  }

  protected Transform[] sourcePartitionSpec() {
    return sourceTable.partitioning();
  }

  protected StagingTableCatalog destCatalog() {
    return destCatalog;
  }

  protected Identifier destTableName() {
    return destTableIdent;
  }

  protected Map<String, String> additionalProperties() {
    return additionalProperties;
  }

  private static void validateSourceTable(CatalogTable sourceTable) {
    String sourceTableProvider = sourceTable.provider().get().toLowerCase(Locale.ROOT);

    if (!ALLOWED_SOURCES.contains(sourceTableProvider)) {
      throw new IllegalArgumentException(
          String.format("Cannot create an Iceberg table from source provider: %s", sourceTableProvider));
    }

    if (sourceTable.storage().locationUri().isEmpty()) {
      throw new IllegalArgumentException("Cannot create an Iceberg table from a source without an explicit location");
    }
  }

  protected static Map<String, String> tableLocationProperties(String tableLocation) {
    return ImmutableMap.of(
        TableProperties.WRITE_METADATA_LOCATION, tableLocation + "/" + ICEBERG_METADATA_FOLDER,
        TableProperties.WRITE_NEW_DATA_LOCATION, tableLocation
    );
  }

  protected static void assignDefaultTableNameMapping(Table table) {
    NameMapping nameMapping = MappingUtil.create(table.schema());
    String nameMappingJson = NameMappingParser.toJson(nameMapping);
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, nameMappingJson).commit();
  }

  private static StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {
    if (!(catalog instanceof SparkSessionCatalog) && !(catalog instanceof SparkCatalog)) {
      throw new IllegalArgumentException(String.format("Cannot create Iceberg table in non Iceberg Catalog. " +
              "Catalog %s was of class %s but %s or %s are required", catalog.name(), catalog.getClass(),
          SparkSessionCatalog.class.getName(), SparkCatalog.class.getName()));
    }
    return (StagingTableCatalog) catalog;
  }

  protected abstract TableCatalog checkSourceCatalog(CatalogPlugin catalog);
}
