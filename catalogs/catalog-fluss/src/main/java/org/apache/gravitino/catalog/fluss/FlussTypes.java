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

import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Fluss data types to Gravitino types and back (the scalar types; nested ones are refused). */
final class FlussTypes {
  private FlussTypes() {}

  static Type fromFluss(DataType t) {
    switch (t.getTypeRoot()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INTEGER:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
        return Types.StringType.get();
      case CHAR:
        return Types.FixedCharType.of(((CharType) t).getLength());
      case DECIMAL:
        return Types.DecimalType.of(((DecimalType) t).getPrecision(), ((DecimalType) t).getScale());
      case DATE:
        return Types.DateType.get();
      case TIME_WITHOUT_TIME_ZONE:
        return Types.TimeType.get();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return Types.TimestampType.withoutTimeZone(((TimestampType) t).getPrecision());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return Types.TimestampType.withTimeZone(((LocalZonedTimestampType) t).getPrecision());
      case BYTES:
        return Types.BinaryType.get();
      case BINARY:
        return Types.FixedType.of(((BinaryType) t).getLength());
      default:
        return Types.ExternalType.of(t.asSummaryString());
    }
  }

  static DataType toFluss(Type t, boolean nullable) {
    DataType d;
    switch (t.name()) {
      case BOOLEAN:
        d = DataTypes.BOOLEAN();
        break;
      case BYTE:
        d = DataTypes.TINYINT();
        break;
      case SHORT:
        d = DataTypes.SMALLINT();
        break;
      case INTEGER:
        d = DataTypes.INT();
        break;
      case LONG:
        d = DataTypes.BIGINT();
        break;
      case FLOAT:
        d = DataTypes.FLOAT();
        break;
      case DOUBLE:
        d = DataTypes.DOUBLE();
        break;
      case STRING:
        d = DataTypes.STRING();
        break;
      case FIXEDCHAR:
        d = DataTypes.CHAR(((Types.FixedCharType) t).length());
        break;
      case DECIMAL:
        d = DataTypes.DECIMAL(((Types.DecimalType) t).precision(), ((Types.DecimalType) t).scale());
        break;
      case DATE:
        d = DataTypes.DATE();
        break;
      case TIME:
        d = DataTypes.TIME();
        break;
      case TIMESTAMP:
        {
          Types.TimestampType ts = (Types.TimestampType) t;
          d =
              ts.hasTimeZone()
                  ? DataTypes.TIMESTAMP_LTZ(ts.precision())
                  : DataTypes.TIMESTAMP(ts.precision());
          break;
        }
      case BINARY:
        d = DataTypes.BYTES();
        break;
      case FIXED:
        d = DataTypes.BINARY(((Types.FixedType) t).length());
        break;
      default:
        throw new IllegalArgumentException("Fluss tables do not support the type " + t);
    }
    return nullable ? d.copy(true) : d.copy(false);
  }
}
