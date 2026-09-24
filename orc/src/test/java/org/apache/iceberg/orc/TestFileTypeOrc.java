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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTypeOrc {
  private static final Types.FileType PHOTO = Types.FileType.of(2);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "photo", PHOTO),
          optional(9, "data", Types.StringType.get()));
  private static final List<Record> ROWS =
      ImmutableList.of(row(1L, fullPhoto(), "a"), row(2L, partialPhoto(), "b"), row(3L, null, "c"));

  @TempDir private Path temp;

  @Test
  void roundTripsAllNestedFields() throws IOException {
    OutputFile file = newOutputFile();
    write(file, SCHEMA, ROWS);

    assertThat(read(file, SCHEMA)).isEqualTo(ROWS);
  }

  @Test
  void readsAProjectionOfASingleNestedField() throws IOException {
    OutputFile file = newOutputFile();
    write(file, SCHEMA, ROWS);
    Schema projection = SCHEMA.select("photo.uri");
    Types.StructType projectedPhoto = projection.findType("photo").asStructType();

    List<Record> expected = Lists.newArrayList();
    for (Record row : ROWS) {
      Record projected = GenericRecord.create(projection);
      Record photo = (Record) row.getField("photo");
      if (photo != null) {
        Record uri = GenericRecord.create(projectedPhoto);
        uri.setField("uri", photo.getField("uri"));
        projected.setField("photo", uri);
      }

      expected.add(projected);
    }

    assertThat(read(file, projection)).isEqualTo(expected);
  }

  @Test
  void collectsMetricsOnlyForDefaultFileFields() throws IOException {
    DataFile dataFile = write(newOutputFile(), SCHEMA, ROWS);
    int photoId = SCHEMA.findField("photo").fieldId();
    int uriId = PHOTO.field("uri").fieldId();
    int sizeId = PHOTO.field("size").fieldId();
    int contentTypeId = PHOTO.field("content_type").fieldId();
    int dataId = SCHEMA.findField("data").fieldId();

    assertThat(dataFile.nullValueCounts())
        .isEqualTo(
            ImmutableMap.of(
                1, 0L, photoId, 1L, uriId, 0L, sizeId, 0L, contentTypeId, 2L, dataId, 0L));
    assertThat(dataFile.lowerBounds()).containsOnlyKeys(1, uriId, sizeId, contentTypeId, dataId);
    assertThat(dataFile.upperBounds()).containsOnlyKeys(1, uriId, sizeId, contentTypeId, dataId);
  }

  private static Record row(long id, Record photo, String data) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("photo", photo);
    row.setField("data", data);
    return row;
  }

  private static Record fullPhoto() {
    return photo(
        ImmutableMap.of(
            "uri",
            "s3://bucket/full",
            "offset",
            128L,
            "size",
            1024L,
            "content_type",
            "image/png",
            "checksum",
            "deadbeef",
            "inline",
            ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8))));
  }

  private static Record partialPhoto() {
    return photo(ImmutableMap.of("uri", "s3://bucket/partial", "size", 8L));
  }

  private static Record photo(Map<String, Object> values) {
    return GenericRecord.create(PHOTO.asStruct()).copy(values);
  }

  private OutputFile newOutputFile() throws IOException {
    File file = File.createTempFile("test", ".orc", temp.toFile());
    assertThat(file.delete()).isTrue();
    return Files.localOutput(file);
  }

  private static DataFile write(OutputFile file, Schema schema, List<Record> rows)
      throws IOException {
    DataWriter<Record> writer =
        ORC.writeData(file)
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .metricsConfig(MetricsConfig.from(ImmutableMap.of(), schema, SortOrder.unsorted()))
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (writer) {
      for (Record row : rows) {
        writer.write(row);
      }
    }

    return writer.toDataFile();
  }

  private static List<Record> read(OutputFile file, Schema projection) throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(projection)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(projection, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
