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

import oracle.kv.table.FieldValue;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.ENUM to Hive column type STRING.
 */
public class TableEnumObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableStringObjectInspector {

    TableEnumObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
        return o == null ?
            new Text(TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE.toString()) :
            new Text(((FieldValue) o).asEnum().get());
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
        if (o == null) {
            return TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE.toString();
        }

        /*
         * Note: it appears that for some queries, Hive leaves the field
         * being processed in its raw, un-deserialized FieldValue form;
         * retrieved from the Row of the KVStore table. Whereas in other
         * cases, the field is deserialized (by the SerDe class) into its
         * corresponding Java primitive form. Thus, depending on the
         * particular query being processed, the parameter input to this
         * method by the Hive infrastructure may be an instance of FieldValue,
         * or may be an instance of the Java primitive class corresponding
         * to the FieldValue represented by this class. As a result, this
         * method must be prepared to handle both cases.
         */
        if (o instanceof Enum) {
            return ((Enum<?>) o).toString();
        } else if (o instanceof FieldValue) {
            return ((FieldValue) o).asEnum().get();
        }
        throw new IllegalArgumentException(
                      "object is not of type Enum or type FieldValue");
    }

    @Override
    public Object create(Text value) {
      return value == null ? TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE :
             TableFieldTypeEnum.stringToEnumValue(value.toString());
    }

    @Override
    public Object set(Object o, Text value) {
      return value == null ? TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE :
             TableFieldTypeEnum.stringToEnumValue(value.toString());
    }

    @Override
    public Object create(String value) {
        return value == null ? TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE :
               TableFieldTypeEnum.stringToEnumValue(value);
    }

    @Override
    public Object set(Object o, String value) {
        return value == null ? TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE :
               TableFieldTypeEnum.stringToEnumValue(value);
    }
}
