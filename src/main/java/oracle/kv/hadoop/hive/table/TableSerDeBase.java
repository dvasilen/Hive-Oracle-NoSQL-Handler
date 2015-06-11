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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.ParamConstant;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Abstract Hive SerDe class that performs deserialization and/or
 * serialization of data loaded into a KVStore via the Table API.
 */
@SuppressWarnings("deprecation")
abstract class TableSerDeBase implements SerDe {

    private static final String thisClassName = TableSerDeBase.class.getName();

    private static final Log LOG = LogFactory.getLog(thisClassName);

    /* Fields shared by sub-classes */
    private KVStore kvStore = null;
    private TableAPI kvTableApi = null;
    private Table kvTable = null;
    private List<String> kvFieldNames = null;
    private List<FieldDef> kvFieldDefs = null;
    private List<FieldDef.Type> kvFieldTypes = null;

    private String hiveTableName = null;

    private LazySerDeParameters serdeParams;
    private ObjectInspector objInspector;

    /* Holds results of deserialization. */
    protected List<Object> hiveRow;

    /* Holds results of serialization. */
    protected MapWritable kvMapWritable;

    /*
     * Transient fields that do not survive serialization/deserialization.
     * The initial values for these fields are obtained from the TBLPROPERTIES
     * that are specified when the table is created for the first time on the
     * Hive client side. Since neither the values of these fields, nor the
     * value of TBLPROPERTIES will survive serialization/deserialization during
     * Hive query MapReduce processing, a static counterpart is specified for
     * each of these fields; where, during initialization, the value of each
     * such static field is set to the same initial value as its transient
     * counterpart. Then, when this class is reinitialized on the server side,
     * since the static fields will survive serialization/deserialization,
     * each such transient field uses its static counterpart (instead of
     * TBLPROPERTIES) to recover its corresponding value from the client
     * side.
     */
    private String kvStoreName = null;
    private String[] kvHelperHosts = null;
    private String[] kvHadoopHosts = null;
    private String kvTableName = null;
    private String kvStoreSecurityFile = null;

    /*
     * Static fields that survive serialization/deserialization. These fields
     * are the counterparts to the transient fields above.
     */
    private static String staticKvStoreName = null;
    private static String[] staticKvHelperHosts = null;
    private static String[] staticKvHadoopHosts = null;
    private static String staticKvTableName = null;
    private static String staticKvStoreSecurityFile = null;

    /* Implementation-specific methods each sub-class must provide. */

    protected abstract void validateParams(Properties tbl)
                                throws SerDeException;

    protected abstract ObjectInspector createObjectInspector()
                                           throws SerDeException;

    /*
     * Methods required by BOTH the org.apache.hadoop.hive.serde2.Deserializer
     * interface and the org.apache.hadoop.hive.serde2.Serializer interface.
     * See org.apache.hadoop.hive.serde2.SerDe.
     */

    @Override
    public void initialize(Configuration job, Properties tbl)
                    throws SerDeException {
        initKvStoreParams(tbl);
        serdeParams = initSerdeParams(job, tbl);
        /* For degugging. */
        displayInitParams(serdeParams);
        validateParams(tbl);
        objInspector = createObjectInspector();
        hiveRow = new ArrayList<Object>();
        kvMapWritable = new MapWritable();
    }

    /**
     * Returns statistics collected when deserializing and/or serializing.
     */
    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    /* Required by the org.apache.hadoop.hive.serde2.Deserializer interface. */

    /**
     * Deserializes the given Writable parameter and returns a Java Object
     * representing the contents of that parameter. The field parameter
     * of this method references a field from a row of the KVStore table
     * having the name specified by the tableName field of this class. Thus,
     * the Object returned by this method references the contents of that
     * table field.
     *
     * @param field The Writable object containing a serialized object from
     *        a row of the KVStore table with name specified by tableName.
     * @return A Java object representing the contents in the given table
     *         field parameter.
     */
    @Override
    public abstract Object deserialize(Writable field) throws SerDeException;

    /**
     * Returns the ObjectInspector that can be used to navigate through the
     * internal structure of the Object returned from the deserialize method.
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objInspector;
    }


    /* Required by the org.apache.hadoop.hive.serde2.Serializer interface. */

    /**
     * Return the Writable class returned by the serialize method; which is
     * used to initialize the SequenceFile header.
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    /**
     * Serialize the given Object by navigating inside the Object with the
     * given ObjectInspector. The given Object references a Hive row and
     * the return value is an instance of Writable that references a
     * KVStore table fieldName:fieldValue pair.
     *
     * @param obj The Object whose contents are examined and from which the
     *            return value is constructed.
     * @param objectInspector The object to use to navigate the given Object's
     *                        contents.
     * @return A Writable object representing the contents in the given
     *         Object to seriaize.
     */
    @Override
    public abstract Writable serialize(Object obj,
                                       ObjectInspector objectInspector)
                                           throws SerDeException;

    @Override
    public String toString() {
        return "[" +
          "kvStoreName=" + getKvStoreName() +
          ":" +
          "kvHelperHosts=" + Arrays.asList(getKvHelperHosts()) +
          ":" +
          "kvHadoopHosts=" + Arrays.asList(getKvHadoopHosts()) +
          ":" +
          "kvTableName=" + getKvTableName() +
          ":" +
          "kvFieldNames=" + getKvFieldNames() +
          ":" +
          "kvFieldTypes=" + getKvFieldTypes() +
          ":" +
          "hiveTableName=" + getHiveTableName() +
          ":" +
          "hiveSeparators=" + getSeparatorsStr(serdeParams) +
          ":" +
          "hiveColumnNames=" + ((StructTypeInfo) serdeParams.getRowTypeInfo())
              .getAllStructFieldNames() +
          ":" +
          "hiveColumnTypes=" + ((StructTypeInfo) serdeParams.getRowTypeInfo())
              .getAllStructFieldTypeInfos() +
          "]";
    }

    String getKvStoreName() {
        return kvStoreName;
    }

    String[] getKvHelperHosts() {
        return kvHelperHosts;
    }

    String[] getKvHadoopHosts() {
        return kvHadoopHosts;
    }

    String getKvTableName() {
        return kvTableName;
    }

    TableAPI getKvTableApi() {
        return kvTableApi;
    }

    Table getKvTable() {
        return kvTable;
    }

    List<String> getKvFieldNames() {
        return kvFieldNames;
    }

    List<FieldDef> getKvFieldDefs() {
        return kvFieldDefs;
    }

    List<FieldDef.Type> getKvFieldTypes() {
        return kvFieldTypes;
    }

    LazySerDeParameters getSerdeParams() {
        return serdeParams;
    }

    String getHiveTableName() {
        return hiveTableName;
    }

    /**
     * Convenience method that return the byte value of the given number
     * String; where if the given defaultVal is returned if the given number
     * String is not a number.
     */
    static byte getByte(String altValue, byte defaultVal) {
        if (altValue != null && altValue.length() > 0) {
            try {
                return Byte.valueOf(altValue).byteValue();
            } catch (NumberFormatException e) {
                return (byte) altValue.charAt(0);
            }
        }
        return defaultVal;
      }

    /**
     * Convenience method that returns a comma-separated String consisting
     * of the Hive column separators specified in the given SerDeParameters.
     */
    String getSeparatorsStr(LazySerDeParameters params) {
        final StringBuilder buf = new StringBuilder("[");
        if (params != null) {
            final byte[] seps = params.getSeparators();
            for (int i = 0; i < seps.length; i++) {
                buf.append(seps[i]);
                if (i < seps.length - 1) {
                    buf.append(",");
                }
            }
        }
        buf.append("]");
        return buf.toString();
    }

    private void initKvStoreParams(Properties tbl) throws SerDeException {

        kvStoreName = tbl.getProperty(ParamConstant.KVSTORE_NAME.getName());
        if (kvStoreName == null) {
            kvStoreName = staticKvStoreName;
            if (kvStoreName == null) {
                final String msg =
                    "No KV Store name specified. Specify the store name via " +
                    "the '" + ParamConstant.KVSTORE_NAME.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        } else {
            staticKvStoreName = kvStoreName;
        }

        final String helperHosts =
                tbl.getProperty(ParamConstant.KVSTORE_NODES.getName());
        if (helperHosts != null) {
            kvHelperHosts = helperHosts.trim().split(",");
            staticKvHelperHosts = helperHosts.trim().split(",");
        } else {
            if (staticKvHelperHosts != null) {
                kvHelperHosts = new String[staticKvHelperHosts.length];
                for (int i = 0; i < staticKvHelperHosts.length; i++) {
                    kvHelperHosts[i] = staticKvHelperHosts[i];
                }
            } else {
                final String msg =
                    "No KV Store helper hosts specified. Specify the helper " +
                    "hosts via the '" + ParamConstant.KVSTORE_NODES.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        }

        final String hadoopHosts =
                tbl.getProperty(ParamConstant.KVHADOOP_NODES.getName());
        if (hadoopHosts != null) {
            kvHadoopHosts = hadoopHosts.trim().split(",");
            staticKvHadoopHosts = hadoopHosts.trim().split(",");
        } else {
            if (staticKvHadoopHosts != null) {
                kvHadoopHosts = new String[staticKvHadoopHosts.length];
                for (int i = 0; i < staticKvHadoopHosts.length; i++) {
                    kvHadoopHosts[i] = staticKvHadoopHosts[i];
                }
            } else {
                kvHadoopHosts = new String[kvHelperHosts.length];
                staticKvHadoopHosts = new String[staticKvHelperHosts.length];
                for (int i = 0; i < kvHelperHosts.length; i++) {
                    /* Strip off the ':port' suffix */
                    final String[] hostPort =
                        (kvHelperHosts[i]).trim().split(":");
                    final String[] staticHostPort =
                        (staticKvHelperHosts[i]).trim().split(":");
                    kvHadoopHosts[i] = hostPort[0];
                    staticKvHadoopHosts[i] = staticHostPort[0];
                }
            }
        }

        kvTableName = tbl.getProperty(ParamConstant.TABLE_NAME.getName());
        if (kvTableName == null) {
            kvTableName = staticKvTableName;
            if (kvTableName == null) {
                final String msg =
                    "No KV Table name specified. Specify the table name via " +
                    "the '" + ParamConstant.TABLE_NAME.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        } else {
            staticKvTableName = kvTableName;
        }

        final String kvStoreSecurityStr =
                  tbl.getProperty(ParamConstant.KVSTORE_SECURITY.getName());
        if (kvStoreSecurityStr != null) {
            kvStoreSecurityFile = kvStoreSecurityStr;
            staticKvStoreSecurityFile = kvStoreSecurityStr;
        } else {
            if (staticKvStoreSecurityFile != null) {
                kvStoreSecurityFile = staticKvStoreSecurityFile;
            }
        }

        final KVStoreConfig kvStoreConfig =
                new KVStoreConfig(kvStoreName, kvHelperHosts);
        kvStoreConfig.setSecurityProperties(
                KVStoreLogin.createSecurityProperties(kvStoreSecurityFile));
        kvStore = KVStoreFactory.getStore(kvStoreConfig);

        kvTableApi = kvStore.getTableAPI();
        kvTable = kvTableApi.getTable(kvTableName);
        if (kvTable == null) {
            final String msg =
                "Store does not contain table [name=" + kvTableName + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        kvFieldNames = kvTable.getFields();
        kvFieldDefs = new ArrayList<FieldDef>();
        kvFieldTypes = new ArrayList<FieldDef.Type>();
        for (String fieldName : kvFieldNames) {
            final FieldDef fieldDef = kvTable.getField(fieldName);
            kvFieldDefs.add(fieldDef);
            kvFieldTypes.add(fieldDef.getType());
        }
    }

    private LazySerDeParameters initSerdeParams(Configuration job, Properties tbl)
                        throws SerDeException {
        hiveTableName =
            tbl.getProperty(hive_metastoreConstants.META_TABLE_NAME);
        /*
         * The above returns a value of the form: 'databaseName.tableName'.
         * For the logging output below, strip off the database name prefix.
         */
        final int indx = hiveTableName.indexOf(".");
        if (indx >= 0) {
            hiveTableName = hiveTableName.substring(indx + 1);
        }
        return new LazySerDeParameters(job, tbl, thisClassName);
    }

    /*
     * Convenience method for debugging.
     */
    private void displayInitParams(LazySerDeParameters params) {
        LOG.debug("kvStoreName = " + getKvStoreName());
        LOG.debug("kvHelperHosts = " + Arrays.asList(getKvHelperHosts()));
        LOG.debug("kvHadoopHosts = " + Arrays.asList(getKvHadoopHosts()));
        LOG.debug("kvTableName = " + getKvTableName());
        LOG.debug("kvFieldNames = " + getKvFieldNames());
        LOG.debug("kvFieldTypes = " + getKvFieldTypes());
        LOG.debug("hiveTableName = " + getHiveTableName());
        LOG.debug("hiveSeparators = " + getSeparatorsStr(params));
        LOG.debug("hiveColumnNames = " + params.getColumnNames());
        LOG.debug("hiveColumnTypes = " + params.getColumnTypes());
        LOG.debug("nullSequence = " + params.getNullSequence());
        LOG.debug("lastColumnTakesRest = " + params.isLastColumnTakesRest());
        LOG.debug("isEscaped = " + params.isEscaped());
        LOG.debug("escapeChar = " + params.getEscapeChar());
    }
}
