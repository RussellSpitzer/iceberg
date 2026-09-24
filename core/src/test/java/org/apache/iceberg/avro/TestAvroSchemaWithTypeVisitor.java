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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestAvroSchemaWithTypeVisitor {

  @Test
  void visitsFileAsStruct() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));
    StructCollector collector = new StructCollector();

    AvroSchemaWithTypeVisitor.visit(schema, AvroSchemaUtil.convert(schema, "table"), collector);

    assertThat(collector.structs)
        .containsExactly(Types.FileType.of(2).asStruct(), schema.asStruct());
  }

  private static class StructCollector extends AvroSchemaWithTypeVisitor<Void> {
    private final List<Types.StructType> structs = Lists.newArrayList();

    @Override
    public Void record(
        Types.StructType iStruct, Schema record, List<String> names, List<Void> fields) {
      structs.add(iStruct);
      return null;
    }
  }
}
