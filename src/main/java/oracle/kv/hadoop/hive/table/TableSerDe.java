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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Row;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Concrete implementation of TableSerDeBase that performs deserialization
 * and/or serialization of data loaded into a KVStore via the PrimaryKey
 * based Table API.
 */
public class TableSerDe extends TableSerDeBase {

    private static final String thisClassName = TableSerDe.class.getName();

    private static final Log LOG = LogFactory.getLog(thisClassName);

    /* Implementation-specific methods required by the parent class. */

    /**
     * Verifies that the names and types of the fields in the KV Store
     * table correctly map to the names and types of the Hive table
     * against which the Hive query is to be executed. Note that this
     * method assumes that both the KVStore parameters and the serde
     * parameters have been initialized. If a mismatch is found between
     * KV Store fields and Hive columns, then a SerDeException will be
     * thrown with a descriptive message.
     */
    @Override
    protected void validateParams(Properties tbl) throws SerDeException {

        LOG.debug("KV Store Table Name = " + getKvTableName());

        final List<String> fieldNames = getKvFieldNames();
        if (fieldNames == null || fieldNames.size() == 0) {
            final String msg =
                "No fields defined in KV Store table [name=" +
                getKvTableName() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }
        LOG.debug("KV Store Field Names = " + fieldNames);

        final List<FieldDef.Type> fieldTypes = getKvFieldTypes();
        if (fieldTypes == null || fieldTypes.size() == 0) {
            final String msg =
                "No types defined for fields in KV Store table [name=" +
                getKvTableName() + ", fields=" + fieldNames + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }
        LOG.debug("KV Store Field Types = " + fieldTypes);

        LOG.debug("HIVE Table Name = " + getHiveTableName());

        final LazySerDeParameters params = getSerdeParams();
        if (params == null) {
            final String msg =
                "No SerDeParameters specified for Hive table [name=" +
                getHiveTableName() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        final List<String> columnNames = params.getColumnNames();
        if (columnNames == null || columnNames.size() == 0) {
            final String msg =
                "No columns defined in Hive table [name=" +
                getHiveTableName() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }
        LOG.debug("HIVE Column Names = " + columnNames);

        final List<TypeInfo> columnTypes = params.getColumnTypes();
        if (columnTypes == null || columnTypes.size() == 0) {
            final String msg =
                "No types defined for columns in Hive table [name=" +
                getHiveTableName() + ", columns=" + columnNames + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }
        LOG.debug("HIVE Column Types = " + columnTypes);

        if (fieldNames.size() != fieldTypes.size()) {
            final String msg =
                "For the KV Store table [name=" + getKvTableName() + "], " +
                "the number of field names [" + fieldNames.size() +
                "] != number of field types [" + fieldTypes.size() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        if (columnNames.size() != columnTypes.size()) {
            final String msg =
                "For the created Hive table [name=" + getHiveTableName() +
                "], the number of column names [" + columnNames.size() +
                "] != number of column types [" + columnTypes.size() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        if (fieldNames.size() != columnNames.size()) {
            final String msg =
                "Number of fields [" + fieldNames.size() + "] in the " +
                "KV Store table [name=" + getKvTableName() + "] != " +
                "number of columns [" + columnNames.size() +
                "] specified for the created Hive table [name=" +
                getHiveTableName() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        /* The field names and column names must be the same. */
        for (String fieldName : fieldNames) {
            if (!columnNames.contains(fieldName.toLowerCase())) {
                final String msg =
                    "Field names from the KV Store table [name=" +
                    getKvTableName() + "] does not match the column names " +
                    "from the created Hive table [name=" + getHiveTableName() +
                    "] - " + fieldNames + " != " + columnNames;
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        }

        /* The field types and column types must be the same. */
        for (int i = 0; i < fieldTypes.size(); i++) {
            if (!TableFieldTypeEnum.fromKvType(fieldTypes.get(i)).equals(
                 TableFieldTypeEnum.fromHiveType(columnTypes.get(i)))) {
                final String msg =
                    "Field types from the KV Store table [name=" +
                    getKvTableName() + "] does not match the column types " +
                    "from the created Hive table [name=" + getHiveTableName() +
                    "] - " + fieldTypes + " != " + columnTypes;
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        }
    }

    @Override
    protected ObjectInspector createObjectInspector() throws SerDeException {

        final List<ObjectInspector> fieldObjInspectors =
            new ArrayList<ObjectInspector>();

        final List<String> hiveColumnNames = getSerdeParams().getColumnNames();

        final List<FieldDef> fieldDefs = getKvFieldDefs();
        for (FieldDef fieldDef : fieldDefs) {
            fieldObjInspectors.add(doCreateObjectInspector(fieldDef));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                   hiveColumnNames, fieldObjInspectors);
    }

    @Override
    public Object deserialize(Writable field) throws SerDeException {
        hiveRow.clear();
        final Row kvRow = getKvTable().createRowFromJson(
                                           field.toString(), true);
        /*
         * For each FieldValue of the given Row, return the corresponding
         * "Hive friendly" value; for example, return the Java primitive
         * value referenced by the given FieldValue.
         */
        for (String fieldName : kvRow.getFields()) {

            final FieldValue fieldValue = kvRow.get(fieldName);
            final TableFieldTypeEnum fieldTypeEnum =
                (fieldValue.isNull() ? TableFieldTypeEnum.TABLE_FIELD_NULL :
                 TableFieldTypeEnum.fromKvType(fieldValue.getType()));

            switch (fieldTypeEnum) {
            case TABLE_FIELD_STRING:
                hiveRow.add(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector.
                        getPrimitiveJavaObject(fieldValue));
                break;

            case TABLE_FIELD_BOOLEAN:
                hiveRow.add(TableFieldTypeEnum.tableBooleanObjectInspector.get(
                                              fieldValue));
                break;

            case TABLE_FIELD_INTEGER:
                hiveRow.add(TableFieldTypeEnum.tableIntObjectInspector.get(
                                              fieldValue));
                break;

            case TABLE_FIELD_LONG:
                hiveRow.add(TableFieldTypeEnum.tableLongObjectInspector.get(
                                              fieldValue));
                break;

            case TABLE_FIELD_FLOAT:
                hiveRow.add(TableFieldTypeEnum.tableFloatObjectInspector.get(
                                              fieldValue));
                break;

            case TABLE_FIELD_DOUBLE:
                hiveRow.add(TableFieldTypeEnum.tableDoubleObjectInspector.get(
                                              fieldValue));
                break;

            case TABLE_FIELD_ENUM:
                hiveRow.add(TableFieldTypeEnum.tableEnumObjectInspector.
                                          getPrimitiveJavaObject(fieldValue));
                break;

            case TABLE_FIELD_BINARY:
            case TABLE_FIELD_FIXED_BINARY:
                hiveRow.add(TableFieldTypeEnum.tableBinaryObjectInspector.
                                          getPrimitiveJavaObject(fieldValue));
                break;

            case TABLE_FIELD_MAP:
            case TABLE_FIELD_RECORD:
                hiveRow.add(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector.
                        getPrimitiveJavaObject(fieldValue));
                break;

            case TABLE_FIELD_ARRAY:
                hiveRow.add(PrimitiveObjectInspectorFactory.
                    javaByteArrayObjectInspector.getPrimitiveJavaObject(
                                                     fieldValue));
                break;

            case TABLE_FIELD_NULL:
                hiveRow.add(null);
                break;

            default:
                hiveRow.add(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector.
                        getPrimitiveJavaObject(fieldValue));
                break;
            }
        }
        return hiveRow;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objectInspector)
                        throws SerDeException {
        final StructObjectInspector structInspector =
            (StructObjectInspector) objectInspector;

        final List<? extends StructField> structFields =
            structInspector.getAllStructFieldRefs();

        final List<String> hiveColumnNames = getSerdeParams().getColumnNames();

        if (structFields.size() != hiveColumnNames.size()) {
            final String msg =
                "Number of Hive columns to serialize " + structFields.size() +
                "] does not equal number of columns [" +
                hiveColumnNames.size() + "] specified in the created Hive " +
                "table [name=" + getHiveTableName() + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        kvMapWritable.clear();

        for (int i = 0; i < structFields.size(); i++) {

            final StructField structField = structFields.get(i);
            final String hiveColumnName = hiveColumnNames.get(i);

            if (structField != null) {

                /* Currently assume field is Hive primitive type. */

                final AbstractPrimitiveObjectInspector fieldObjInspector =
                    (AbstractPrimitiveObjectInspector)
                                       structField.getFieldObjectInspector();

                final Object fieldData =
                    structInspector.getStructFieldData(obj, structField);

                Writable fieldValue =
                    (Writable) fieldObjInspector.getPrimitiveWritableObject(
                                                     fieldData);
                if (fieldValue == null) {
                    if (PrimitiveCategory.STRING.equals(
                            fieldObjInspector.getPrimitiveCategory())) {

                        fieldValue = NullWritable.get();
                    } else {
                        fieldValue = new IntWritable(0);
                    }
                }

                kvMapWritable.put(new Text(hiveColumnName), fieldValue);
            }
        }
        return kvMapWritable;
    }

    private ObjectInspector doCreateObjectInspector(FieldDef fieldDef) {
        final TableFieldTypeEnum type =
            TableFieldTypeEnum.fromKvType(fieldDef.getType());

        switch(type) {
            case TABLE_FIELD_MAP:
                return doCreateMapObjectInspector(fieldDef);
            case TABLE_FIELD_RECORD:
                break;
            case TABLE_FIELD_ARRAY:
                return doCreateArrayObjectInspector();
           default:
                return type.getObjectInspector();
        }
        return type.getObjectInspector();
    }

    private ObjectInspector doCreateMapObjectInspector(FieldDef fieldDef) {
        /* Currently assume 1-level Map of String to common primitive type. */
        final FieldDef elementDef = ((MapDef) fieldDef).getElement();
        final TableFieldTypeEnum elementType =
            TableFieldTypeEnum.fromKvType(elementDef.getType());

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                   PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                   elementType.getObjectInspector());
    }

    /*
     * Note: not currently used in 1st release. Included for future reference.
     * Remove the annotation when this method is eventually used.
     */
    @SuppressWarnings("unused")
    private ObjectInspector doCreateRecordObjectInspector(FieldDef
                                tableFieldDef) throws SerDeException {

        /* Currently assume 1-level Record of different primitive types. */
        final List<String> recFieldNames =
                               ((RecordDef) tableFieldDef).getFields();
        final List<ObjectInspector> recObjInspectors =
                                        new ArrayList<ObjectInspector>();
        for (String recFieldName : recFieldNames) {
            final FieldDef recFieldDef =
                      ((RecordDef) tableFieldDef).getField(recFieldName);
            final TableFieldTypeEnum recFieldType =
                      TableFieldTypeEnum.fromKvType(recFieldDef.getType());
            recObjInspectors.add(recFieldType.getObjectInspector());
        }
        return ObjectInspectorFactory.getStandardUnionObjectInspector(
                                          recObjInspectors);
    }

    private ObjectInspector doCreateArrayObjectInspector() {

        /* Assume 1-level array of elements of same primitive type. */
        return ObjectInspectorFactory.getStandardListObjectInspector(
                   PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
}
