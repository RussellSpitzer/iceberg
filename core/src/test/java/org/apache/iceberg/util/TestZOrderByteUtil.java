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


package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.primitives.UnsignedBytes;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestZOrderByteUtil {
  private static final byte IIIIIIII = (byte) 255;
  private static final byte IOIOIOIO = (byte) 170;
  private static final byte OIOIOIOI = (byte) 85;
  private static final byte OOOOIIII = (byte) 15;
  private static final byte OOOOOOOI = (byte) 1;
  private static final byte OOOOOOOO = (byte) 0;

  private static final int NUM_TESTS = 100000;

  private final Random random = new Random(42);

  private String bytesToString(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte b : bytes) {
      result.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
    }
    return result.toString();
  }

  /**
   * Returns a non-0 length byte array
   */
  private byte[]  generateRandomBytes() {
    int length = Math.abs(random.nextInt(100) + 1);
    byte[] result = new byte[length];
    random.nextBytes(result);
    return result;
  }

  /**
   * Test method to ensure correctness of byte interleaving code
   */
  private String interleaveStrings(String[] strings) {
    StringBuilder result = new StringBuilder();
    int totalLength = Arrays.stream(strings).mapToInt(String::length).sum();
    int substringIndex = 0;
    int characterIndex = 0;
    while (characterIndex < totalLength) {
      for (String str : strings) {
        if (substringIndex < str.length()) {
          result.append(str.charAt(substringIndex));
          characterIndex++;
        }
      }
      substringIndex++;
    }
    return result.toString();
  }

  /**
   * Compares the result of a string based interleaving algorithm implemented above
   * versus the binary bit-shifting algorithm used in ZOrderByteUtils. Either both
   * algorithms are identically wrong or are both identically correct.
   */
  @Test
  public void testInterleaveRandomExamples() {
    for (int test = 0; test < NUM_TESTS; test++) {
      int numByteArrays = Math.abs(random.nextInt(6)) + 1;
      byte[][] testBytes =  new byte[numByteArrays][];
      String[] testStrings = new String[numByteArrays];
      for (int byteIndex = 0;  byteIndex < numByteArrays; byteIndex++) {
        testBytes[byteIndex] = generateRandomBytes();
        testStrings[byteIndex] = bytesToString(testBytes[byteIndex]);
      }
      byte[] byteResult = ZOrderByteUtils.interleaveBits(testBytes);
      String byteResultAsString = bytesToString(byteResult);

      String stringResult = interleaveStrings(testStrings);

      Assert.assertEquals("String interleave didn't match byte interleave", stringResult, byteResultAsString);
    }
  }

  @Test
  public void testInterleaveEmptyBits() {
    byte[][] test = new byte[4][10];
    byte[] expected = new byte[40];

    Assert.assertArrayEquals("Should combine empty arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testInterleaveFullBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[]{IIIIIIII, IIIIIIII};
    test[1] = new byte[]{IIIIIIII};
    test[2] = new byte[0];
    test[3] = new byte[]{IIIIIIII, IIIIIIII, IIIIIIII};
    byte[] expected = new byte[]{IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII};

    Assert.assertArrayEquals("Should combine full arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testInterleaveMixedBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[]{OOOOOOOI, IIIIIIII, OOOOOOOO, OOOOIIII};
    test[1] = new byte[]{OOOOOOOI, OOOOOOOO, IIIIIIII};
    test[2] = new byte[]{OOOOOOOI};
    test[3] = new byte[]{OOOOOOOI};
    byte[] expected = new byte[]{
        OOOOOOOO, OOOOOOOO, OOOOOOOO, OOOOIIII,
        IOIOIOIO, IOIOIOIO,
        OIOIOIOI, OIOIOIOI,
        OOOOIIII};
    Assert.assertArrayEquals("Should combine mixed byte arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testIntOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      int aInt = random.nextInt();
      int bInt = random.nextInt();
      int intCompare = Integer.signum(Integer.compare(aInt, bInt));
      byte[] aBytes = ZOrderByteUtils.intToOrderedBytes(aInt);
      byte[] bBytes = ZOrderByteUtils.intToOrderedBytes(bInt);
      int byteCompare = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(String.format(
          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aInt, bInt, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          intCompare, byteCompare);
    }
  }

  @Test
  public void testLongOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      long aLong = random.nextInt();
      long bLong = random.nextInt();
      int longCompare = Integer.signum(Long.compare(aLong, bLong));
      byte[] aBytes = ZOrderByteUtils.longToOrderBytes(aLong);
      byte[] bBytes = ZOrderByteUtils.longToOrderBytes(bLong);
      int byteCompare = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(String.format(
          "Ordering of longs should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aLong, bLong, longCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          longCompare, byteCompare);
    }
  }

  @Test
  public void testFloatOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      float aFloat = random.nextFloat();
      float bFloat = random.nextFloat();
      int floatCompare = Integer.signum(Float.compare(aFloat, bFloat));
      byte[] aBytes = ZOrderByteUtils.floatToOrderedBytes(aFloat);
      byte[] bBytes = ZOrderByteUtils.floatToOrderedBytes(bFloat);
      int byteCompare = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(String.format(
          "Ordering of floats should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aFloat, bFloat, floatCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          floatCompare, byteCompare);
    }
  }

  @Test
  public void testDoubleOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      double aDouble = random.nextDouble();
      double bDouble = random.nextDouble();
      int doubleCompare = Integer.signum(Double.compare(aDouble, bDouble));
      byte[] aBytes = ZOrderByteUtils.doubleToOrderedBytes(aDouble);
      byte[] bBytes = ZOrderByteUtils.doubleToOrderedBytes(bDouble);
      int byteCompare = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(String.format(
          "Ordering of doubles should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aDouble, bDouble, doubleCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          doubleCompare, byteCompare);
    }
  }

  @Test
  public void testStringOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      String aString =  (String) RandomUtil.generatePrimitive(Types.StringType.get(), random);
      String bString =  (String) RandomUtil.generatePrimitive(Types.StringType.get(), random);
      int stringCompare = Integer.signum(aString.compareTo(bString));
      byte[] aBytes = ZOrderByteUtils.stringToOrderedBytes(aString, 128);
      byte[] bBytes = ZOrderByteUtils.stringToOrderedBytes(bString, 128);
      int byteCompare = Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(String.format(
          "Ordering of strings should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aString, bString, stringCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          stringCompare, byteCompare);
    }
  }
}
