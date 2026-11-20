/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class FlussTypesTest {

  private static void roundTrip(Type gravitino, DataTypeRoot root) {
    DataType fluss = FlussTypes.toFluss(gravitino, true);
    assertEquals(root, fluss.getTypeRoot());
    assertEquals(gravitino, FlussTypes.fromFluss(fluss), "back to " + gravitino);
  }

  @Test
  void simpleTypesRoundTrip() {
    roundTrip(Types.BooleanType.get(), DataTypeRoot.BOOLEAN);
    roundTrip(Types.ByteType.get(), DataTypeRoot.TINYINT);
    roundTrip(Types.ShortType.get(), DataTypeRoot.SMALLINT);
    roundTrip(Types.IntegerType.get(), DataTypeRoot.INTEGER);
    roundTrip(Types.LongType.get(), DataTypeRoot.BIGINT);
    roundTrip(Types.FloatType.get(), DataTypeRoot.FLOAT);
    roundTrip(Types.DoubleType.get(), DataTypeRoot.DOUBLE);
    roundTrip(Types.StringType.get(), DataTypeRoot.STRING);
    roundTrip(Types.DateType.get(), DataTypeRoot.DATE);
    roundTrip(Types.BinaryType.get(), DataTypeRoot.BYTES);
  }

  @Test
  void parameterisedTypesRoundTrip() {
    roundTrip(Types.DecimalType.of(12, 3), DataTypeRoot.DECIMAL);
    roundTrip(Types.FixedCharType.of(8), DataTypeRoot.CHAR);
    roundTrip(Types.FixedType.of(16), DataTypeRoot.BINARY);
  }

  @Test
  void timestampPrecisionIsKept() {
    for (int p : new int[] {0, 3, 6, 9}) {
      DataType ntz = FlussTypes.toFluss(Types.TimestampType.withoutTimeZone(p), true);
      assertEquals(p, ((TimestampType) ntz).getPrecision());
      assertEquals(Types.TimestampType.withoutTimeZone(p), FlussTypes.fromFluss(ntz));
      DataType ltz = FlussTypes.toFluss(Types.TimestampType.withTimeZone(p), true);
      assertEquals(p, ((LocalZonedTimestampType) ltz).getPrecision());
      assertEquals(Types.TimestampType.withTimeZone(p), FlussTypes.fromFluss(ltz));
    }
  }

  @Test
  void unspecifiedTimestampPrecisionDefaultsToSix() {
    // Gravitino reports "no precision given" as a negative number
    Type plain = Types.TimestampType.withoutTimeZone();
    assertTrue(((Types.TimestampType) plain).precision() < 0);
    assertEquals(6, ((TimestampType) FlussTypes.toFluss(plain, true)).getPrecision());
    Type tz = Types.TimestampType.withTimeZone();
    assertEquals(6, ((LocalZonedTimestampType) FlussTypes.toFluss(tz, true)).getPrecision());
  }

  @Test
  void nullabilityIsCarried() {
    assertTrue(FlussTypes.toFluss(Types.LongType.get(), true).isNullable());
    assertFalse(FlussTypes.toFluss(Types.LongType.get(), false).isNullable());
  }

  @Test
  void unsupportedTypesAreRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> FlussTypes.toFluss(Types.ListType.of(Types.StringType.get(), true), true));
    assertThrows(
        IllegalArgumentException.class, () -> FlussTypes.toFluss(Types.UUIDType.get(), true));
  }

  @Test
  void unknownFlussTypesComeBackAsExternal() {
    Type t = FlussTypes.fromFluss(DataTypes.ARRAY(DataTypes.INT()));
    assertTrue(t instanceof Types.ExternalType);
  }
}
