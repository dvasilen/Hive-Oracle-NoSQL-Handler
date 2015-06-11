/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2015 Oracle and/or its affiliates.  All rights reserved.
 *
 * If you have received this file as part of Oracle NoSQL Database the
 * following applies to the work as a whole:
 *
 *   Oracle NoSQL Database server software is free software: you can
 *   redistribute it and/or modify it under the terms of the GNU Affero
 *   General Public License as published by the Free Software Foundation,
 *   version 3.
 *
 *   Oracle NoSQL Database is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   Affero General Public License for more details.
 *
 * If you have received this file as part of Oracle NoSQL Database Client or
 * distributed separately the following applies:
 *
 *   Oracle NoSQL Database client software is free software: you can
 *   redistribute it and/or modify it under the terms of the Apache License
 *   as published by the Apache Software Foundation, version 2.0.
 *
 * You should have received a copy of the GNU Affero General Public License
 * and/or the Apache License in the LICENSE file along with Oracle NoSQL
 * Database client or server distribution.  If not, see
 * <http://www.gnu.org/licenses/>
 * or
 * <http://www.apache.org/licenses/LICENSE-2.0>.
 *
 * An active Oracle commercial licensing agreement for this product supersedes
 * these licenses and in such case the license notices, but not the copyright
 * notice, may be removed by you in connection with your distribution that is
 * in accordance with the commercial licensing terms.
 *
 * For more information please contact:
 *
 * berkeleydb-info_us@oracle.com
 *
 */

package oracle.kv.hadoop.hive.table;

import oracle.kv.table.FieldDef;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Enum class that defines values corresponding to each of the enum
 * values defined in FieldDef.Type; which represent the possible field
 * types of a KV Store table. The methods of this enum provide a
 * mechanism for mapping a table defined in a given KV Store and a
 * table created in Hive.
 */
public enum TableFieldTypeEnum {

    TABLE_FIELD_STRING {
        @Override
        public ObjectInspector getObjectInspector() {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.STRING.toString();
        }
    },
    TABLE_FIELD_BOOLEAN {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableBooleanObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.BOOLEAN.toString();
        }
    },
    TABLE_FIELD_INTEGER {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableIntObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.INTEGER.toString();
        }
    },
    TABLE_FIELD_LONG {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableLongObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.LONG.toString();
        }
    },
    TABLE_FIELD_FLOAT {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableFloatObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.FLOAT.toString();
        }
    },
    TABLE_FIELD_DOUBLE {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableDoubleObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.DOUBLE.toString();
        }
    },
    TABLE_FIELD_ENUM {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableEnumObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.ENUM.toString();
        }
    },
    TABLE_FIELD_BINARY {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableBinaryObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.BINARY.toString();
        }
    },
    TABLE_FIELD_FIXED_BINARY {
        @Override
        public ObjectInspector getObjectInspector() {
            return tableBinaryObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.FIXED_BINARY.toString();
        }
    },
    TABLE_FIELD_MAP {
        @Override
        public ObjectInspector getObjectInspector() {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.MAP.toString();
        }
    },
    TABLE_FIELD_RECORD {
        @Override
        public ObjectInspector getObjectInspector() {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.RECORD.toString();
        }
    },
    TABLE_FIELD_ARRAY {
        @Override
        public ObjectInspector getObjectInspector() {
            return
                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public String toString() {
            return FieldDef.Type.ARRAY.toString();
        }
    },
    TABLE_FIELD_NULL {
        @Override
        public ObjectInspector getObjectInspector() {
            return null;
        }

        @Override
        public String toString() {
            return "NULL kv table field";
        }
    },
    TABLE_FIELD_UNKNOWN_TYPE {
        @Override
        public ObjectInspector getObjectInspector() {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

        @Override
        public String toString() {
            return "unknown kv table field type";
        }
    };

    abstract ObjectInspector getObjectInspector();

    public static final TableBooleanObjectInspector
        tableBooleanObjectInspector = new TableBooleanObjectInspector();
    public static final TableIntObjectInspector
        tableIntObjectInspector = new TableIntObjectInspector();
    public static final TableLongObjectInspector
        tableLongObjectInspector = new TableLongObjectInspector();
    public static final TableFloatObjectInspector
        tableFloatObjectInspector = new TableFloatObjectInspector();
    public static final TableDoubleObjectInspector
        tableDoubleObjectInspector = new TableDoubleObjectInspector();
    public static final TableEnumObjectInspector
        tableEnumObjectInspector = new TableEnumObjectInspector();
    public static final TableBinaryObjectInspector
        tableBinaryObjectInspector = new TableBinaryObjectInspector();

    /**
     * Maps the given field type of a KV Store table to the corresponding
     * enum value defined in this class; corresponding to a field type of
     * a KV Store table.
     */
    public static TableFieldTypeEnum fromKvType(FieldDef.Type kvType) {
        return stringToEnumValue(kvType.toString());
    }

    /**
     * Maps the given Hive column type to the corresponding enum value
     * defined in this class; corresponding to a field type of a KV Store
     * table. Note that some of the Hive types have no corresponding type;
     * in which case, TABLE_FIELD_UNKNOWN_TYPE is returned.
     */
    public static TableFieldTypeEnum fromHiveType(TypeInfo hiveType) {
        if (serdeConstants.BOOLEAN_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_BOOLEAN;
        }
        if (serdeConstants.INT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_INTEGER;
        }
        if (serdeConstants.BIGINT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_LONG;
        }
        if (serdeConstants.FLOAT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_FLOAT;
        }
        if (serdeConstants.DECIMAL_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_FLOAT;
        }
        if (serdeConstants.DOUBLE_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_DOUBLE;
        }
        if (serdeConstants.STRING_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_STRING;
        }
        if (serdeConstants.BINARY_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_BINARY;
        }
        if (serdeConstants.MAP_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_MAP;
        }
        if (serdeConstants.LIST_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_ARRAY;
        }
        if (serdeConstants.STRUCT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_RECORD;
        }

        /* The following Hive types have no match in KV table types. */

        if (serdeConstants.TINYINT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.SMALLINT_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.DATE_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.DATETIME_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.TIMESTAMP_TYPE_NAME.equals(
                                                   hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.UNION_TYPE_NAME.equals(hiveType.getTypeName())) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        return TABLE_FIELD_UNKNOWN_TYPE;
    }

    public static TableFieldTypeEnum stringToEnumValue(String str) {
        for (TableFieldTypeEnum enumVal : TableFieldTypeEnum.values()) {
            if (enumVal.toString().equals(str)) {
                return enumVal;
            }
        }
        final String msg = "no enum value " + TableFieldTypeEnum.class +
                           "." + str;
        throw new IllegalArgumentException(msg);
    }
}
