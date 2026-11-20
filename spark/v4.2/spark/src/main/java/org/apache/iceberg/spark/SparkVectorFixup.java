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

final class SparkVectorFixup {
  private SparkVectorFixup() {}

  static Types.StructType fixup(
      Types.StructType converted, Types.StructType base, boolean caseSensitive) {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(converted.fields().size());
    for (Types.NestedField field : converted.fields()) {
      Types.NestedField baseField =
          caseSensitive ? base.field(field.name()) : base.caseInsensitiveField(field.name());
      Type fixed =
          baseField != null
              ? fixup(field.name(), field.type(), baseField.type(), caseSensitive)
              : null;
      fields.add(
          fixed != null && fixed != field.type()
              ? Types.NestedField.from(field).ofType(fixed).build()
              : field);
    }

    return Types.StructType.of(fields);
  }

  private static Type fixup(String name, Type converted, Type base, boolean caseSensitive) {
    if (base.isVectorType() && converted.isListType()) {
      Types.VectorType vector = base.asVectorType();
      Type element = converted.asListType().elementType();
      Preconditions.checkArgument(
          element.equals(valueType(vector.elementType())),
          "Cannot write %s to %s column %s",
          converted,
          vector,
          name);
      return vector;
    } else if (converted.isStructType() && base.isStructType()) {
      return fixup(converted.asStructType(), base.asStructType(), caseSensitive);
    } else if (converted.isListType() && base.isListType()) {
      Types.ListType list = converted.asListType();
      Type element =
          fixup(name, list.elementType(), base.asListType().elementType(), caseSensitive);
      return list.isElementOptional()
          ? Types.ListType.ofOptional(list.elementId(), element)
          : Types.ListType.ofRequired(list.elementId(), element);
    } else if (converted.isMapType() && base.isMapType()) {
      Types.MapType map = converted.asMapType();
      Type value = fixup(name, map.valueType(), base.asMapType().valueType(), caseSensitive);
      return map.isValueOptional()
          ? Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), value)
          : Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), value);
    }

    return converted;
  }

  // Spark ByteType converts to IntegerType, so int8 vectors are matched against int elements
  private static Type valueType(Types.VectorType.ElementType elementType) {
    return switch (elementType) {
      case INT8, INT -> Types.IntegerType.get();
      case LONG -> Types.LongType.get();
      case FLOAT16, FLOAT -> Types.FloatType.get();
      case DOUBLE -> Types.DoubleType.get();
    };
  }
}
