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
import oracle.kv.table.FieldValue;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.FLOAT to Hive column type FLOAT.
 */
public class TableFloatObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableFloatObjectInspector {

    TableFloatObjectInspector() {
        super(TypeInfoFactory.floatTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        return o == null ? null : new FloatWritable(get(o));
    }

    @Override
    public float get(Object o) {
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
        if (o instanceof Float) {
            return ((Float) o).floatValue();
        } else if (o instanceof FieldValue) {
            return ((FieldValue) o).asFloat().get();
        }
        throw new IllegalArgumentException(
                      "object is not of type Float or type FieldValue");
    }

    @Override
    public Object create(float value) {
        /*
         * It appears that a FloatValue cannot currently be created from the
         * given primitive value. One would like to be able to do something
         * like:
         *     return FieldValue.createFloat(value);
         * Unfortunately, this does not seem to be currently possile with
         * the KVStore Table API. Thus, 'stub out' this method by returning
         * a Float for now.
         */
        return Float.valueOf(value);
    }

    @Override
    public Object set(Object o, float value) {
        /*
         * This method assumes that the given Object is a FieldDef; although
         * it would be preferable if the Object were a FieldValue instead.
         * Unfortunately, FieldValue currently defines 'getDefinition' only
         * for the FieldValue types: EnumValue, FixedBinaryValue, ArrayValue,
         * MapValue, and RecordValue; otherwise, if all FieldValue types
         * defined such a 'getDefinition' method, then this method could
         * assume the given Object is a FieldValue and do something like
         * the following:
         *
         *    return ((FieldValue) o).getDefinition().createFloat(value);
         *
         * For now, 'stub out' this method by assuming the given Object is
         * a FieldDef and use the 'create<FieldType>' method of the given
         * FieldDef to generate the expected return value.
         */
        return ((FieldDef) o).createFloat(value);
    }
}
