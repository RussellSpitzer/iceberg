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
package org.apache.iceberg.spark;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Restores vector columns when converting a Spark write schema to an Iceberg schema.
 *
 * <p>Spark has no vector type, so a {@code vector(E, D)} column is exposed to Spark as an array of
 * E's Spark element type (see {@link TypeToSparkType}): {@code tinyint} for int8, {@code int} for
 * int, {@code bigint} for long, {@code float} for float16 and float, and {@code double} for double.
 * Converting a Spark schema back produces an Iceberg list for every array, and ID reassignment
 * against the table schema rejects a list where the table has a vector.
 *
 * <p>This fixup runs on the converted schema before ID reassignment. It replaces each list with the
 * table's vector type when the incoming Spark array's element type is exactly the vector's Spark
 * element type, and fails with an error naming the column otherwise. The Spark type is compared,
 * rather than the converted Iceberg type, because Spark's {@code tinyint}, {@code smallint}, and
 * {@code int} all convert to Iceberg {@code int}. The vector dimension and element values are
 * checked when rows are written.
 */
final class SparkVectorElementFixup {
  private SparkVectorElementFixup() {}

  static Types.StructType fixup(
      Types.StructType converted,
      StructType sparkType,
      Types.StructType base,
      boolean caseSensitive) {
    List<Types.NestedField> convertedFields = converted.fields();
    StructField[] sparkFields = sparkType.fields();
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(convertedFields.size());
    for (int i = 0; i < convertedFields.size(); i += 1) {
      Types.NestedField field = convertedFields.get(i);
      Types.NestedField baseField =
          caseSensitive ? base.field(field.name()) : base.caseInsensitiveField(field.name());
      Type fixed =
          baseField != null
              ? fixup(
                  field.name(),
                  field.type(),
                  sparkFields[i].dataType(),
                  baseField.type(),
                  caseSensitive)
              : field.type();
      fields.add(
          fixed != field.type() ? Types.NestedField.from(field).ofType(fixed).build() : field);
    }

    return Types.StructType.of(fields);
  }

  private static Type fixup(
      String name, Type converted, DataType sparkType, Type base, boolean caseSensitive) {
    if (base.typeId() == Type.TypeID.VECTOR && converted.isListType()) {
      Types.VectorType vector = (Types.VectorType) base;
      DataType expectedElement = TypeToSparkType.vectorElementType(vector.elementType());
      Preconditions.checkArgument(
          ((ArrayType) sparkType).elementType().equals(expectedElement),
          "Cannot write %s to %s column %s",
          sparkType.catalogString(),
          vector,
          name);
      return vector;
    } else if (converted.isStructType() && base.isStructType()) {
      return fixup(
          converted.asStructType(), (StructType) sparkType, base.asStructType(), caseSensitive);
    } else if (converted.isListType() && base.isListType()) {
      Types.ListType list = converted.asListType();
      Type element =
          fixup(
              name,
              list.elementType(),
              ((ArrayType) sparkType).elementType(),
              base.asListType().elementType(),
              caseSensitive);
      return list.isElementOptional()
          ? Types.ListType.ofOptional(list.elementId(), element)
          : Types.ListType.ofRequired(list.elementId(), element);
    } else if (converted.isMapType() && base.isMapType()) {
      Types.MapType map = converted.asMapType();
      Type value =
          fixup(
              name,
              map.valueType(),
              ((MapType) sparkType).valueType(),
              base.asMapType().valueType(),
              caseSensitive);
      return map.isValueOptional()
          ? Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), value)
          : Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), value);
    }

    return converted;
  }
}
