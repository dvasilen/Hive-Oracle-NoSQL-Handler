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

import java.util.Arrays;

import oracle.kv.table.BinaryDef;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.FixedBinaryValue;

import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of either type FieldDef.Type.BINARY or type FieldDef.Type.Fixed_BINARY
 * to Hive column type BINARY.
 */
public class TableBinaryObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableBinaryObjectInspector {

    TableBinaryObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    @Override
    public byte[] copyObject(Object o) {
        if (o instanceof BinaryValue) {
            return ((BinaryValue) o).clone().get();
        } else if (o instanceof FixedBinaryValue) {
            return ((FixedBinaryValue) o).clone().get();
        }
        return null;
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
        if (o instanceof BinaryValue) {
            return new BytesWritable(((BinaryValue) o).get());
        } else if (o instanceof FixedBinaryValue) {
            return new BytesWritable(((FixedBinaryValue) o).get());
        }
        return null;
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
        if (o instanceof BinaryValue) {
            return ((BinaryValue) o).get();
        } else if (o instanceof FixedBinaryValue) {
            return ((FixedBinaryValue) o).get();
        }
        return null;
    }

    @Override
    public byte[] set(Object o, byte[] bb) {
        if (null == bb) {
            return null;
        }

        if (o instanceof BinaryDef) {
            return ((FieldDef) o).createBinary(bb).get();
        } else if (o instanceof FixedBinaryDef) {
            return ((FieldDef) o).createFixedBinary(bb).get();
        }
        return null;
    }

    @Override
    public byte[] set(Object o, BytesWritable bw) {
        if (null == bw) {
            return null;
        }

        if (o instanceof BinaryDef) {
            return ((FieldDef) o).createBinary(
                                      LazyUtils.createByteArray(bw)).get();
        } else if (o instanceof FixedBinaryDef) {
           return
               ((FieldDef) o).createFixedBinary(
                                  LazyUtils.createByteArray(bw)).get();
        }
        return null;
    }

    @Override
    public byte[] create(byte[] bb) {
        return bb == null ? null : Arrays.copyOf(bb, bb.length);
    }

    @Override
    public byte[] create(BytesWritable bw) {
        return bw == null ? null : LazyUtils.createByteArray(bw);
    }
}
