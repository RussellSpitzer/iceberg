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
package org.apache.iceberg.parquet;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.VectorType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

final class ParquetVectors {
  static final String LIST_NAME = "list";
  static final String ELEMENT_NAME = "element";

  private static final int FLOAT16_LENGTH = 2;
  private static final int INT8_BIT_WIDTH = 8;

  private ParquetVectors() {}

  static boolean isVector(GroupType group) {
    return group.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation;
  }

  static int dimension(GroupType vector) {
    return ((FiniteVectorLogicalTypeAnnotation) vector.getLogicalTypeAnnotation()).getNumElements();
  }

  static PrimitiveType element(GroupType vector) {
    Preconditions.checkArgument(
        vector.getFieldCount() == 1,
        "Invalid vector: expected a single repeated group: %s",
        vector);
    Type repeated = vector.getType(0);
    Preconditions.checkArgument(
        !repeated.isPrimitive()
            && repeated.isRepetition(Type.Repetition.REPEATED)
            && repeated.asGroupType().getFieldCount() == 1,
        "Invalid vector: expected a repeated group with a single element field: %s",
        vector);
    Type element = repeated.asGroupType().getType(0);
    Preconditions.checkArgument(
        element.isPrimitive(), "Invalid vector: element must be primitive: %s", vector);
    Preconditions.checkArgument(
        element.isRepetition(Type.Repetition.REQUIRED),
        "Invalid vector: element must be required: %s",
        vector);
    return element.asPrimitiveType();
  }

  static GroupType toParquet(VectorType vector, Type.Repetition repetition, int id, String name) {
    return Types.buildGroup(repetition)
        .as(LogicalTypeAnnotation.finiteVectorType(vector.dimension()))
        .repeatedGroup()
        .addField(toParquet(vector.elementType()))
        .named(LIST_NAME)
        .id(id)
        .named(name);
  }

  private static PrimitiveType toParquet(VectorType.ElementType elementType) {
    return switch (elementType) {
      case INT8 ->
          Types.required(PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.intType(INT8_BIT_WIDTH, true))
              .named(ELEMENT_NAME);
      case INT -> Types.required(PrimitiveTypeName.INT32).named(ELEMENT_NAME);
      case LONG -> Types.required(PrimitiveTypeName.INT64).named(ELEMENT_NAME);
      case FLOAT16 ->
          Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              .length(FLOAT16_LENGTH)
              .as(LogicalTypeAnnotation.float16Type())
              .named(ELEMENT_NAME);
      case FLOAT -> Types.required(PrimitiveTypeName.FLOAT).named(ELEMENT_NAME);
      case DOUBLE -> Types.required(PrimitiveTypeName.DOUBLE).named(ELEMENT_NAME);
    };
  }

  static VectorType.ElementType elementType(PrimitiveType element) {
    LogicalTypeAnnotation annotation = element.getLogicalTypeAnnotation();
    switch (element.getPrimitiveTypeName()) {
      case INT32:
        if (annotation instanceof IntLogicalTypeAnnotation intType
            && intType.getBitWidth() == INT8_BIT_WIDTH
            && intType.isSigned()) {
          return VectorType.ElementType.INT8;
        } else if (annotation == null
            || (annotation instanceof IntLogicalTypeAnnotation intType
                && intType.getBitWidth() == Integer.SIZE
                && intType.isSigned())) {
          return VectorType.ElementType.INT;
        }

        break;
      case INT64:
        if (annotation == null
            || (annotation instanceof IntLogicalTypeAnnotation intType
                && intType.getBitWidth() == Long.SIZE
                && intType.isSigned())) {
          return VectorType.ElementType.LONG;
        }

        break;
      case FIXED_LEN_BYTE_ARRAY:
        if (annotation instanceof Float16LogicalTypeAnnotation
            && element.getTypeLength() == FLOAT16_LENGTH) {
          return VectorType.ElementType.FLOAT16;
        }

        break;
      case FLOAT:
        return VectorType.ElementType.FLOAT;
      case DOUBLE:
        return VectorType.ElementType.DOUBLE;
      default:
        break;
    }

    throw new UnsupportedOperationException("Unsupported vector element type: " + element);
  }

  static float readFloat16(Binary binary) {
    byte[] bytes = binary.getBytesUnsafe();
    return float16ToFloat((short) ((bytes[0] & 0xFF) | (bytes[1] << 8)));
  }

  /** Encodes a float as little-endian binary16 into the given 2-byte buffer. */
  static Binary writeFloat16(float value, byte[] buffer) {
    short bits = floatToFloat16(value);
    buffer[0] = (byte) bits;
    buffer[1] = (byte) (bits >> 8);
    return Binary.fromReusedByteArray(buffer);
  }

  static float float16ToFloat(short bits) {
    return Float16Conversions.toFloat(bits);
  }

  /**
   * Converts a float to IEEE 754 binary16 bits.
   *
   * @throws IllegalArgumentException if the value is not finite or not exactly representable
   */
  static short floatToFloat16(float value) {
    Preconditions.checkArgument(
        Float.isFinite(value), "Invalid float16 vector element: %s (must be finite)", value);

    short result = Float16Conversions.toFloat16(value);
    Preconditions.checkArgument(
        Float.floatToRawIntBits(Float16Conversions.toFloat(result))
            == Float.floatToRawIntBits(value),
        "Invalid float16 vector element: %s (not exactly representable as float16)",
        value);

    return result;
  }
}
