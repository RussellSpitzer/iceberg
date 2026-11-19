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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.VectorType.ElementType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSparkVectors extends TestBase {
  private static final String CATALOG = "vector_catalog";
  private static final String TABLE = CATALOG + ".default.vectors";
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "vectors");
  private static final String WAREHOUSE =
      new File(System.getProperty("java.io.tmpdir"), "iceberg_spark_vector_warehouse")
          .toURI()
          .toString();

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "int8_vec", Types.VectorType.of(ElementType.INT8, 3)),
          optional(3, "f16_vec", Types.VectorType.of(ElementType.FLOAT16, 3)),
          optional(4, "float_vec", Types.VectorType.of(ElementType.FLOAT, 2)));

  private static HadoopCatalog catalog;

  @BeforeAll
  static void setupCatalog() {
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".cache-enabled", "false");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE);
    catalog = new HadoopCatalog(spark.sessionState().newHadoopConf(), WAREHOUSE);
  }

  @BeforeEach
  void createTable() {
    catalog.dropTable(TABLE_IDENT);
    catalog.createTable(
        TABLE_IDENT,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of(TableProperties.FORMAT_VERSION, "4"));
  }

  @AfterEach
  void dropTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void insertAndSelect(boolean vectorized) {
    sql(
        "INSERT INTO %s VALUES "
            + "(1, array(-128Y, 0Y, 127Y), array(1.5F, -0.25F, 65504F), array(0.1F, 2.0F)), "
            + "(2, NULL, NULL, NULL)",
        TABLE);
    setVectorization(vectorized);

    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", TABLE);

    assertEquals(
        "Vectors should round-trip through Spark",
        ImmutableList.of(
            row(
                1L,
                List.of((byte) -128, (byte) 0, (byte) 127),
                List.of(1.5F, -0.25F, 65504.0F),
                List.of(0.1F, 2.0F)),
            row(2L, null, null, null)),
        rows);
  }

  private void setVectorization(boolean enabled) {
    catalog
        .loadTable(TABLE_IDENT)
        .updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(enabled))
        .commit();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selectWithoutVectorColumns(boolean vectorized) {
    sql("INSERT INTO %s VALUES (1, array(1Y, 2Y, 3Y), array(1F, 2F, 3F), array(1F, 2F))", TABLE);
    setVectorization(vectorized);

    assertEquals(
        "Non-vector columns should be readable",
        ImmutableList.of(row(1L)),
        sql("SELECT id FROM %s", TABLE));
  }

  @Test
  void sparkSchemaUsesArrays() {
    assertThat(spark.table(TABLE).schema().simpleString())
        .isEqualTo(
            "struct<id:bigint,int8_vec:array<tinyint>,f16_vec:array<float>,"
                + "float_vec:array<float>>");
  }

  @Test
  void insertRejectsWrongDimension() {
    assertThatThrownBy(
            () ->
                sql(
                    "INSERT INTO %s VALUES (1, array(1Y, 2Y), array(1F, 2F, 3F), array(1F, 2F))",
                    TABLE))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid vector: expected 3 elements but got 2");
  }

  @Test
  void insertRejectsInexactFloat16() {
    assertThatThrownBy(
            () ->
                sql(
                    "INSERT INTO %s VALUES (1, array(1Y, 2Y, 3Y), array(1F, 0.1F, 3F), array(1F, 2F))",
                    TABLE))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(
            "Invalid float16 vector element: 0.1 (not exactly representable as float16)");
  }
}
