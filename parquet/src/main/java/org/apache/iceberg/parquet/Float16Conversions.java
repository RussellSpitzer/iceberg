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

/**
 * Conversions between IEEE 754 binary16 bits and float.
 *
 * <p>Copied from parquet-java's {@code org.apache.parquet.schema.Float16} ({@code toFloat} and
 * {@code toFloat16}), which are package-private there and are themselves based on Android's {@code
 * libcore.util.FP16}. Local variable names are adapted to Iceberg's checkstyle rules; the logic is
 * unchanged. Remove this class when parquet-java exposes these conversions or Iceberg targets Java
 * 20 or later, which provides {@code Float.float16ToFloat} and {@code Float.floatToFloat16}.
 */
final class Float16Conversions {
  private static final int SIGN_MASK = 0x8000;
  private static final int EXPONENT_SHIFT = 10;
  private static final int SHIFTED_EXPONENT_MASK = 0x1f;
  private static final int SIGNIFICAND_MASK = 0x3ff;
  private static final int EXPONENT_BIAS = 15;
  private static final int SIGN_SHIFT = 15;
  private static final int FP32_SIGN_SHIFT = 31;
  private static final int FP32_EXPONENT_SHIFT = 23;
  private static final int FP32_SHIFTED_EXPONENT_MASK = 0xff;
  private static final int FP32_SIGNIFICAND_MASK = 0x7fffff;
  private static final int FP32_EXPONENT_BIAS = 127;
  private static final int FP32_QNAN_MASK = 0x400000;
  private static final int FP32_DENORMAL_MAGIC = 126 << 23;
  private static final float FP32_DENORMAL_FLOAT = Float.intBitsToFloat(FP32_DENORMAL_MAGIC);

  private Float16Conversions() {}

  /** Converts binary16 bits to a float. Signaling NaNs are converted to quiet NaNs. */
  static float toFloat(short half) {
    int bits = half & 0xffff;
    int sign = bits & SIGN_MASK;
    int exponent = (bits >>> EXPONENT_SHIFT) & SHIFTED_EXPONENT_MASK;
    int significand = (bits) & SIGNIFICAND_MASK;
    int outE = 0;
    int outM = 0;
    if (exponent == 0) { // Denormal or 0
      if (significand != 0) {
        // Convert denorm fp16 into normalized fp32
        float value = Float.intBitsToFloat(FP32_DENORMAL_MAGIC + significand);
        value -= FP32_DENORMAL_FLOAT;
        return sign == 0 ? value : -value;
      }
    } else {
      outM = significand << 13;
      if (exponent == 0x1f) { // Infinite or NaN
        outE = 0xff;
        if (outM != 0) { // SNaNs are quieted
          outM |= FP32_QNAN_MASK;
        }
      } else {
        outE = exponent - EXPONENT_BIAS + FP32_EXPONENT_BIAS;
      }
    }

    int out = (sign << 16) | (outE << FP32_EXPONENT_SHIFT) | outM;
    return Float.intBitsToFloat(out);
  }

  /**
   * Converts a float to binary16 bits, rounding to nearest even. Values too large for binary16
   * become infinity and values too small become zero.
   */
  static short toFloat16(float value) {
    int bits = Float.floatToRawIntBits(value);
    int sign = (bits >>> FP32_SIGN_SHIFT);
    int exponent = (bits >>> FP32_EXPONENT_SHIFT) & FP32_SHIFTED_EXPONENT_MASK;
    int significand = (bits) & FP32_SIGNIFICAND_MASK;
    int outE = 0;
    int outM = 0;
    if (exponent == 0xff) { // Infinite or NaN
      outE = 0x1f;
      outM = significand != 0 ? 0x200 : 0;
    } else {
      exponent = exponent - FP32_EXPONENT_BIAS + EXPONENT_BIAS;
      if (exponent >= 0x1f) { // Overflow
        outE = 0x1f;
      } else if (exponent <= 0) { // Underflow
        if (exponent < -10) {
          // The absolute fp32 value is less than MIN_VALUE, flush to +/-0
        } else {
          // The fp32 value is a normalized float less than MIN_NORMAL,
          // we convert to a denorm fp16
          significand = significand | 0x800000;
          int shift = 14 - exponent;
          outM = significand >> shift;
          int lowm = significand & ((1 << shift) - 1);
          int hway = 1 << (shift - 1);
          // if above halfway or exactly halfway and outM is odd
          if (lowm + (outM & 1) > hway) {
            // Round to nearest even
            // Can overflow into exponent bit, which surprisingly is OK.
            // This increment relies on the +outM in the return statement below
            outM++;
          }
        }
      } else {
        outE = exponent;
        outM = significand >> 13;
        // if above halfway or exactly halfway and outM is odd
        if ((significand & 0x1fff) + (outM & 0x1) > 0x1000) {
          // Round to nearest even
          // Can overflow into exponent bit, which surprisingly is OK.
          // This increment relies on the +outM in the return statement below
          outM++;
        }
      }
    }

    // The outM is added here as the +1 increments for outM above can
    // cause an overflow in the exponent bit which is OK.
    return (short) ((sign << SIGN_SHIFT) | (outE << EXPONENT_SHIFT) + outM);
  }
}
