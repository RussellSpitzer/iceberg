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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.VectorType;
import org.apache.iceberg.types.Types.VectorType.ElementType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestParquetVectors {

  @Test
  void writeRejectsWrongDimension() {
    Schema schema = vectorSchema(ElementType.FLOAT, 3);

    assertThatThrownBy(() -> write(schema, record(schema, Arrays.asList(1.0F, 2.0F))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid vector: expected 3 elements but got 2");
  }

  @Test
  void writeRejectsInexactFloat16() {
    Schema schema = vectorSchema(ElementType.FLOAT16, 2);

    assertThatThrownBy(() -> write(schema, record(schema, Arrays.asList(1.0F, 0.1F))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid float16 vector element: 0.1 (not exactly representable as float16)");
  }

  @Test
  void writeRejectsFloat16Overflow() {
    Schema schema = vectorSchema(ElementType.FLOAT16, 1);

    assertThatThrownBy(() -> write(schema, record(schema, List.of(65536.0F))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid float16 vector element: 65536.0 (not exactly representable as float16)");
  }

  @ParameterizedTest
  @ValueSource(floats = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})
  void writeRejectsNonFiniteFloat16(float value) {
    Schema schema = vectorSchema(ElementType.FLOAT16, 1);

    assertThatThrownBy(() -> write(schema, record(schema, List.of(value))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid float16 vector element: %s (must be finite)", value);
  }

  @ParameterizedTest
  @ValueSource(floats = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})
  void writeRejectsNonFiniteFloat(float value) {
    Schema schema = vectorSchema(ElementType.FLOAT, 1);

    assertThatThrownBy(() -> write(schema, record(schema, List.of(value))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid float vector element: %s (must be finite)", value);
  }

  @ParameterizedTest
  @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
  void writeRejectsNonFiniteDouble(double value) {
    Schema schema = vectorSchema(ElementType.DOUBLE, 1);

    assertThatThrownBy(() -> write(schema, record(schema, List.of(value))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid double vector element: %s (must be finite)", value);
  }

  @Test
  void float16RoundTripPreservesEdgeValues() throws IOException {
    List<Float> values =
        Arrays.asList(
            0.0F, -0.0F, 1.0F, -2.5F, 65504.0F, -65504.0F, 0x1p-14F, 0x1p-24F, -0x1.ff8p-15F);
    Schema schema = vectorSchema(ElementType.FLOAT16, values.size());
    Record expected = record(schema, values);

    List<Record> actual = read(write(schema, expected), schema);

    assertThat(actual).containsExactly(expected);
  }

  @Test
  void readRejectsWrongElementCount() throws IOException {
    Schema listSchema =
        new Schema(required(1, "vec", Types.ListType.ofRequired(2, Types.FloatType.get())));
    OutputFile file = write(listSchema, record(listSchema, Arrays.asList(1.0F, 2.0F)));

    assertThatThrownBy(() -> read(file, vectorSchema(ElementType.FLOAT, 3)))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid vector: expected 3 elements but read 2");
  }

  @Test
  void readRejectsDimensionMismatch() throws IOException {
    Schema schema = vectorSchema(ElementType.FLOAT, 2);
    OutputFile file = write(schema, record(schema, Arrays.asList(1.0F, 2.0F)));

    assertThatThrownBy(() -> read(file, vectorSchema(ElementType.FLOAT, 3)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read vector with dimension 2 as vector(float, 3)");
  }

  @ParameterizedTest
  @MethodSource("promotions")
  void readPromotesElements(
      ElementType fileType, List<?> written, ElementType readType, List<?> expected)
      throws IOException {
    Schema schema = vectorSchema(fileType, written.size());
    OutputFile file = write(schema, record(schema, written));

    Schema readSchema = vectorSchema(readType, written.size());
    assertThat(read(file, readSchema)).containsExactly(record(readSchema, expected));
  }

  private static Stream<Arguments> promotions() {
    List<Byte> int8s = List.of((byte) -128, (byte) 127);
    List<Float> halfs = List.of(1.5F, -0.25F);
    return Stream.of(
        Arguments.of(ElementType.INT8, int8s, ElementType.INT, List.of(-128, 127)),
        Arguments.of(ElementType.INT8, int8s, ElementType.LONG, List.of(-128L, 127L)),
        Arguments.of(ElementType.INT, List.of(1, -2), ElementType.LONG, List.of(1L, -2L)),
        Arguments.of(ElementType.FLOAT16, halfs, ElementType.FLOAT, halfs),
        Arguments.of(ElementType.FLOAT16, halfs, ElementType.DOUBLE, List.of(1.5D, -0.25D)),
        Arguments.of(
            ElementType.FLOAT,
            List.of(0.1F, 2.0F),
            ElementType.DOUBLE,
            List.of((double) 0.1F, 2.0D)));
  }

  @Test
  void readRejectsDisallowedElementType() throws IOException {
    Schema schema = vectorSchema(ElementType.FLOAT, 2);
    OutputFile file = write(schema, record(schema, List.of(1.0F, 2.0F)));

    assertThatThrownBy(() -> read(file, vectorSchema(ElementType.INT, 2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read vector elements of type float as int");
  }

  @Test
  void writeRejectsNullElement() {
    Schema schema = vectorSchema(ElementType.INT, 2);

    assertThatThrownBy(() -> write(schema, record(schema, Arrays.asList(1, null))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid vector: elements must not be null");
  }

  @Test
  void readsListWithRequiredElementsAsVector() throws IOException {
    Schema listSchema =
        new Schema(required(1, "vec", Types.ListType.ofRequired(2, Types.FloatType.get())));
    OutputFile file = write(listSchema, record(listSchema, List.of(1.0F, 2.0F)));

    Schema vectorSchema = vectorSchema(ElementType.FLOAT, 2);
    assertThat(read(file, vectorSchema)).containsExactly(record(vectorSchema, List.of(1.0F, 2.0F)));
  }

  @Test
  void readRejectsListWithOptionalElements() throws IOException {
    Schema listSchema =
        new Schema(required(1, "vec", Types.ListType.ofOptional(2, Types.FloatType.get())));
    OutputFile file = write(listSchema, record(listSchema, Arrays.asList(1.0F, null)));

    assertThatThrownBy(() -> read(file, vectorSchema(ElementType.FLOAT, 2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid vector: element must be required");
  }

  @Test
  void vectorsInStructAndListRoundTrip() throws IOException {
    Types.VectorType vector = VectorType.of(ElementType.FLOAT16, 2);
    Schema schema =
        new Schema(
            optional(1, "struct", Types.StructType.of(optional(2, "vec", vector))),
            optional(3, "vecs", Types.ListType.ofOptional(4, vector)));
    Record struct = GenericRecord.create(schema.findType("struct").asStructType());
    struct.set(0, List.of(1.0F, -2.0F));
    Record first = GenericRecord.create(schema);
    first.set(0, struct);
    first.set(1, Arrays.asList(List.of(0.5F, 0.25F), null, List.of(4.0F, 8.0F)));
    Record second = GenericRecord.create(schema);
    second.set(1, List.of());

    assertThat(read(write(schema, first, second), schema)).containsExactly(first, second);
  }

  @Test
  void float16ConversionMatchesArithmeticDecoding() {
    for (int bits = 0; bits <= 0xFFFF; bits += 1) {
      short half = (short) bits;
      float expected = decodeFloat16(bits);
      float actual = ParquetVectors.float16ToFloat(half);
      if (Float.isNaN(expected)) {
        assertThat(actual).as("bits 0x%04x", bits).isNaN();
      } else {
        assertThat(Float.floatToRawIntBits(actual))
            .as("bits 0x%04x", bits)
            .isEqualTo(Float.floatToRawIntBits(expected));
        if (Float.isFinite(expected)) {
          assertThat(ParquetVectors.floatToFloat16(expected))
              .as("bits 0x%04x", bits)
              .isEqualTo(half);
        }
      }
    }
  }

  @Test
  void float16ConversionRejectsValuesBetweenFloat16Values() {
    float betweenOneAndNext = Math.nextUp(1.0F);
    float belowSmallestSubnormal = 0x1p-25F;
    float betweenSubnormals = 0x1.8p-24F;
    float aboveMax = Math.nextUp(65504.0F);

    for (float value :
        new float[] {betweenOneAndNext, belowSmallestSubnormal, betweenSubnormals, aboveMax}) {
      assertThatThrownBy(() -> ParquetVectors.floatToFloat16(value))
          .as("value %s", value)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not exactly representable as float16");
    }
  }

  @Test
  void float16StoredAsLittleEndianBinary16(@TempDir File temp) throws IOException {
    Schema schema = vectorSchema(ElementType.FLOAT16, 2);
    File file = new File(temp, "float16.parquet");
    writeTo(Files.localOutput(file), schema, record(schema, List.of(1.0F, -2.0F)));

    List<byte[]> stored = Lists.newArrayList();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new Path(file.toURI())).build()) {
      Group vec = reader.read().getGroup("vec", 0);
      for (int i = 0; i < vec.getFieldRepetitionCount("list"); i += 1) {
        stored.add(vec.getGroup("list", i).getBinary("element", 0).getBytes());
      }
    }

    assertThat(stored).containsExactly(new byte[] {0x00, 0x3C}, new byte[] {0x00, (byte) 0xC0});
  }

  @Test
  void int8StoredAsAnnotatedInt32() {
    MessageType parquetSchema =
        ParquetSchemaUtil.convert(vectorSchema(ElementType.INT8, 4), "table");

    PrimitiveType element =
        parquetSchema
            .getType("vec")
            .asGroupType()
            .getType(0)
            .asGroupType()
            .getType(0)
            .asPrimitiveType();
    assertThat(element.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.INT32);
    assertThat(element.getLogicalTypeAnnotation())
        .isEqualTo(LogicalTypeAnnotation.intType(8, true));
  }

  // decodes IEEE 754 binary16 arithmetically, independently of the bit manipulation under test
  private static float decodeFloat16(int bits) {
    int sign = (bits >> 15) & 0x1;
    int exponent = (bits >> 10) & 0x1F;
    int mantissa = bits & 0x3FF;
    float magnitude;
    if (exponent == 0x1F) {
      magnitude = mantissa == 0 ? Float.POSITIVE_INFINITY : Float.NaN;
    } else if (exponent == 0) {
      magnitude = Math.scalb((float) mantissa, -24);
    } else {
      magnitude = Math.scalb(1.0F + mantissa / 1024.0F, exponent - 15);
    }

    return sign == 1 ? -magnitude : magnitude;
  }

  @Test
  void writesFiniteVectorAnnotation() throws IOException {
    Schema schema = vectorSchema(ElementType.FLOAT16, 4);
    OutputFile file = write(schema, record(schema, Arrays.asList(1.0F, 2.0F, 3.0F, 4.0F)));

    MessageType fileSchema;
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file.toInputFile()))) {
      fileSchema = reader.getFileMetaData().getSchema();
    }

    GroupType vector = fileSchema.getType("vec").asGroupType();
    assertThat(vector.getId().intValue()).isEqualTo(1);
    assertThat(vector.getLogicalTypeAnnotation())
        .isEqualTo(LogicalTypeAnnotation.finiteVectorType(4));

    GroupType repeated = vector.getType(0).asGroupType();
    PrimitiveType element = repeated.getType(0).asPrimitiveType();
    assertThat(repeated.getId()).isNull();
    assertThat(element.getId()).isNull();
    assertThat(element.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.float16Type());
    assertThat(element.getTypeLength()).isEqualTo(2);
  }

  @Test
  void schemaConversionRoundTrips() {
    for (ElementType elementType : ElementType.values()) {
      Schema schema = new Schema(optional(1, "vec", VectorType.of(elementType, 8)));

      MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
      assertThat(parquetSchema.getType("vec").getLogicalTypeAnnotation())
          .isInstanceOf(FiniteVectorLogicalTypeAnnotation.class);
      assertThat(ParquetSchemaUtil.convert(parquetSchema).asStruct()).isEqualTo(schema.asStruct());
    }
  }

  @Test
  void nameMappingAssignsVectorId() {
    Schema schema = new Schema(optional(7, "vec", VectorType.of(ElementType.INT8, 4)));
    MessageType withoutIds = RemoveIds.removeIds(ParquetSchemaUtil.convert(schema, "table"));

    MessageType mapped = ParquetSchemaUtil.applyNameMapping(withoutIds, MappingUtil.create(schema));

    assertThat(mapped.getType("vec").getLogicalTypeAnnotation())
        .isEqualTo(LogicalTypeAnnotation.finiteVectorType(4));
    assertThat(ParquetSchemaUtil.convert(mapped).asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  void writerReportsVectorValueAndNullCounts() throws IOException {
    Schema schema = new Schema(optional(1, "vec", VectorType.of(ElementType.FLOAT16, 2)));
    OutputFile outputFile = new InMemoryOutputFile();
    DataWriter<StructLike> writer = dataWriter(schema, outputFile);
    try (writer) {
      writer.write(record(schema, Arrays.asList(1.0F, 2.0F)));
      writer.write(record(schema, null));
      writer.write(record(schema, Arrays.asList(3.0F, 4.0F)));
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile.valueCounts()).containsEntry(1, 3L);
    assertThat(dataFile.nullValueCounts()).containsEntry(1, 1L);
    assertThat(dataFile.lowerBounds())
        .satisfiesAnyOf(
            bounds -> assertThat(bounds).isNull(),
            bounds -> assertThat(bounds).doesNotContainKey(1));
  }

  private static Schema vectorSchema(ElementType elementType, int dimension) {
    return new Schema(required(1, "vec", VectorType.of(elementType, dimension)));
  }

  private static Record record(Schema schema, List<?> value) {
    Record record = GenericRecord.create(schema);
    record.set(0, value);
    return record;
  }

  private static DataWriter<StructLike> dataWriter(Schema schema, OutputFile file)
      throws IOException {
    return Parquet.writeData(file)
        .schema(schema)
        .createWriterFunc(InternalWriter::createWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
  }

  private static OutputFile write(Schema schema, Record... records) throws IOException {
    return writeTo(new InMemoryOutputFile(), schema, records);
  }

  private static OutputFile writeTo(OutputFile file, Schema schema, Record... records)
      throws IOException {
    try (DataWriter<StructLike> writer = dataWriter(schema, file)) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    return file;
  }

  private static List<Record> read(OutputFile file, Schema schema) throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
