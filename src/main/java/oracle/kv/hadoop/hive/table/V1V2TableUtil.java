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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.ParamConstant;
import oracle.kv.hadoop.table.TableInputFormat;
import oracle.kv.hadoop.table.TableInputSplit;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utility class that provides static convenience methods for managing the
 * interactions between version 1 and version 2 (YARN) MapReduce classes.
 */
public final class V1V2TableUtil {

    private static final Object V1_V2_UTIL_LOCK = new Object();

    private static final Log LOG = LogFactory.getLog(
                       "oracle.kv.hadoop.hive.table.V1V2TableUtil");

    private static InputFormat<PrimaryKey, Row> v2InputFormat;

    private static Map<TableHiveInputSplit, TableInputSplit> v1V2SplitMap;

    /**
     * Utility class. Do not allow instantiation.
     */
    private V1V2TableUtil() {
    }

    /**
     * For the current Hive query, returns a singleton collection that
     * maps each version 1 InputSplit for the query to its corresponding
     * version 2 InputSplit. If the call to this method is the first call
     * after the query has been entered on the command line and the input
     * info for the job has been reset (using resetInputJobInfoForNewQuery),
     * this method will construct and populate the return Map; otherwise,
     * it will return the previously constructed Map.
     * <p>
     * Implementation Note: when the getInputFormat method from this class
     * is called to retrieve the TableInputFormat instance, only the VERY FIRST
     * call to getInputFormat will construct an instance of TableInputFormat;
     * all additional calls will always return the original instance created
     * by that first call. More importantly, in addition to constructing a
     * TableInputFormat instance, that first call to getInputFormat also
     * constructs and populates the Map returned by this method; which is
     * achieved via a call to the getSplits method on the newly created
     * TableInputFormat instance.
     * <p>
     * Since the first call to the getInputFormat method of this class has
     * already called TableInputFormat.getSplits and placed the retrieved
     * splits in the Map to return here, it is no longer necessary to make
     * any additional calls to TableInputFormat.getSplits. Not only is it not
     * necessary to call TableInputFormat.getSplits, but such a call should
     * be avoided. This is because any call to TableInputFormat.getSplits
     * will result in remote calls to the KVStore; which can be very costly.
     * As a result, one should NEVER make a call such as,
     * <code>
     *     getInputFormat().getSplits()
     * </code>
     * as such a call may result in two successive calls to
     * TableInputFormat.getSplits. Thus, to avoid the situation just described,
     * this method only needs to call getInputFormat (not getSplits()) to
     * construct and populate the Map to return.
     */
    public static Map<TableHiveInputSplit, TableInputSplit> getSplitMap(
                                               JobConf jobConf,
                                               TableHiveInputSplit inputSplit)
                                                          throws IOException {
        synchronized (V1_V2_UTIL_LOCK) {
            if (v1V2SplitMap == null) {
                /* Construct & populate both the splitMap and v2InputFormat */
                getInputFormat(jobConf, inputSplit);
            }
            return v1V2SplitMap;
        }
    }

    public static Map<TableHiveInputSplit, TableInputSplit> getSplitMap(
                                                          JobConf jobConf)
                                                          throws IOException {
        return getSplitMap(jobConf, null);
    }

    /**
     * For the current Hive query, constructs and returns a YARN based
     * InputFormat class that will be used when processing the query.
     * This method also constructs and populates a singleton Map whose
     * elements are key/value pairs in which each key is a version 1
     * split for the returned InputFormat, and each value is the key's
     * corresponding version 2 split. Note that both the InputFormat and
     * the Map are contructed only on the first call to this method for
     * the given query. On all subsequent calls, the original objects
     * are returned; until the resetInputJobInfoForNewQuery method from
     * this utility is called.
     */
    @SuppressWarnings("deprecation")
    public static InputFormat<PrimaryKey, Row> getInputFormat(
                                             JobConf jobConf,
                                             TableHiveInputSplit inputSplit)
                                  throws IOException {

        Path[] tablePaths = FileInputFormat.getInputPaths(jobConf);
        if (tablePaths == null || tablePaths.length == 0) {
            LOG.debug("FileInputFormat.getInputPaths returned " +
                          (tablePaths == null ? "NULL" : "zero length array"));
            if (inputSplit != null) {
                tablePaths = new Path[] { inputSplit.getPath() };
            } else {
                /*
                 * This should never happen, but if it does, then set the
                 * tablePaths to a value that may help with debugging.
                 */
                tablePaths = new Path[] { new Path("/TABLE_PATHS_NOT_SET") };
            }
        }
        LOG.debug("tablePaths[0] = " + tablePaths[0]);

        getStoreInfo(jobConf, inputSplit);

        synchronized (V1_V2_UTIL_LOCK) {
            if (v2InputFormat == null) {

                /* Instantiate v2InputFormat to return. */
                v2InputFormat = ReflectionUtils.newInstance(
                                         TableInputFormat.class, jobConf);

                /* Construct and populate the v1V2SplitMap. */
                v1V2SplitMap =
                        new HashMap<TableHiveInputSplit, TableInputSplit>();
                final List<InputSplit> v2Splits;
                try {
                    v2Splits = v2InputFormat.getSplits(new Job(jobConf));

                    for (InputSplit curV2Split : v2Splits) {
                        v1V2SplitMap.put(
                            new TableHiveInputSplit(
                                tablePaths[0], (TableInputSplit) curV2Split),
                                (TableInputSplit) curV2Split);
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            return v2InputFormat;
        }
    }

    public static InputFormat<PrimaryKey, Row> getInputFormat(JobConf jobConf)
                                  throws IOException {
        return getInputFormat(jobConf, null);
    }

    /**
     * Clears and resets the information related to the current job's input
     * classes.
     * <p>
     * This method must be called every time a new Hive query has been
     * entered on the command line; to reset the splits as well as the
     * InputFormats participating in the job.
     */
    public static void resetInputJobInfoForNewQuery() {

        synchronized (V1_V2_UTIL_LOCK) {

            if (v1V2SplitMap != null) {
                v1V2SplitMap.clear();
                v1V2SplitMap = null;
            }
            v2InputFormat = null;
        }
    }

    /**
     * Convenience method that sets the properties the current Hive query
     * job needs to connect to and retrieve records from the store;
     * specifically, the name of the store and the store's helper hosts.
     * <p>
     * Implementation Note: if the Hive query to be performed is a
     * "client-side query" -- that is, a query in which the processing
     * occurs only on the Hive client, not via a MapReduce job -- then
     * the values handled by this method should be already set; via the
     * TBLPROPERTIES entered on the Hive command line. On the other
     * hand, if the query is complex enough that the Hive infrastructure
     * must construct and submit a MapReduce job to perform the query,
     * then the values set by this method are obtained from the given
     * InputSplit; which must be Non-Null.
     */
    private static void getStoreInfo(
                            JobConf jobConf, TableHiveInputSplit split) {

        /* 1. Store name */
        String storeName = jobConf.get(ParamConstant.KVSTORE_NAME.getName());
        if (storeName == null) {

            /* Must be MapReduce: get store name from the split */
            storeName = split.getKVStoreName();
            if (storeName != null) {
                jobConf.set(ParamConstant.KVSTORE_NAME.getName(), storeName);
            }
        }

        /* 2. Helper Hosts */
        String hostsStr = jobConf.get(ParamConstant.KVSTORE_NODES.getName());
        if (hostsStr == null) {

            /* Must be MapReduce: get store nodes from the split */
            final String[] hostsArray = split.getKVHelperHosts();
            if (hostsArray != null) {
                final StringBuilder buf = new StringBuilder(hostsArray[0]);
                for (int i = 1; i < hostsArray.length; i++) {
                    buf.append("," + hostsArray[i]);
                }
                hostsStr = buf.toString();

                jobConf.set(ParamConstant.KVSTORE_NODES.getName(), hostsStr);
            }
        }

        /* 3. Table name */
        String tableName = jobConf.get(ParamConstant.TABLE_NAME.getName());
        if (tableName == null) {

            /* Must be MapReduce: get table name from the split */
            tableName = split.getTableName();
            if (tableName != null) {
                jobConf.set(ParamConstant.TABLE_NAME.getName(), tableName);
            }
        }
    }
}
