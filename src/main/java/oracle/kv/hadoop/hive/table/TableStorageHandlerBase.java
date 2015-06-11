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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.ParamConstant;
import oracle.kv.impl.util.ExternalDataSourceUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

/**
 * Abstract class which is used as the base class for Oracle NoSQL Database
 * Hive StorageHandler classes designed to work with data stored via the
 * Table API.
 * <p>
 *
 * For a list of parameters that are recognized, see the javadoc for
 * {@link oracle.kv.hadoop.table.TableInputFormatBase}.
 *
 * @since 3.1
 */
abstract class TableStorageHandlerBase<K extends WritableComparable<?>,
                                       V extends Writable>
                          extends DefaultStorageHandler {

    private static final Log LOG = LogFactory.getLog(
                       "oracle.kv.hadoop.hive.table.TableStorageHandlerBase");

    protected String kvStoreName = null;
    protected String[] kvHelperHosts = null;
    protected String[] kvHadoopHosts = null;
    protected String tableName = null;

    protected String primaryKeyProperty = null;

    /* For MultiRowOptions */
    protected String fieldRangeProperty = null;

    /* For TableIteratorOptions */
    protected Direction direction = Direction.UNORDERED;
    protected Consistency consistency = null;
    protected long timeout = 0;
    protected TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    protected int maxRequests = 0;
    protected int batchSize = 0;
    protected int maxBatches = 0;

    protected String kvStoreSecurityFile = null;

    protected JobConf jobConf = new JobConf();

    /**
     * @hidden
     */
    protected TableStorageHandlerBase() {
    }

    /**
     * Creates a configuration for job input. This method provides the
     * mechanism for populating this StorageHandler's configuration (returned
     * by <code>JobContext.getConfiguration</code>) with the properties that
     * may be needed by the handler's bundled artifacts; for example, the
     * <code>InputFormat</code> class, the <code>SerDe</code> class, etc.
     * returned by this handler. Any key value pairs set in the
     * <code>jobProperties</code> argument are guaranteed to be set in the
     * job's configuration object; and any "context" information associated
     * with the job can be retrieved from the given <code>TableDesc</code>
     * parameter.
     * <p>
     * Note that implementations of this method must be idempotent. That
     * is, when this method is invoked more than once with the same
     * <code>tableDesc</code> values for a given job, the key value pairs
     * in <code>jobProperties</code>, as well as any external state set
     * by this method, should be the same after each invocation. How this
     * invariant guarantee is achieved is left as an implementation detail;
     * although to support this guarantee, changes should only be made
     * to the contents of <code>jobProperties</code>, but never to
     * <code>tableDesc</code>.
     */
    @Override
    public void configureInputJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        V1V2TableUtil.resetInputJobInfoForNewQuery();
        configureJobProperties(tableDesc, jobProperties);
    }

    /**
     * Using semantics identical to the semantics of the
     *<code>configureInputJobProperties</code> method, creates a
     * configuration for job output. For more detail, refer to the
     * description of the <code>configureInputJobProperties</code> method.
     */
    @Override
    public void configureOutputJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        configureJobProperties(tableDesc, jobProperties);
    }

    /**
     * Although this method was originally intended to configure
     * properties for a job based on the definition of the source or
     * target table the job accesses, this method is now deprecated
     * in Hive. The methods <code>configureInputJobProperties</code>
     * and <code>configureOutputJobProperties</code> should be used
     * instead.
     */
    @Override
    public void configureTableJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc,
                                        Map<String, String> jobProperties) {

        final Properties tableProperties = tableDesc.getProperties();

        kvStoreName =
            tableProperties.getProperty(ParamConstant.KVSTORE_NAME.getName());
        if (kvStoreName == null) {
            throw new IllegalArgumentException
                ("No KV Store Name provided via the '" +
                 ParamConstant.KVSTORE_NAME.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        LOG.debug("kvStoreName = " + kvStoreName);

        jobProperties.put(ParamConstant.KVSTORE_NAME.getName(), kvStoreName);
        jobConf.set(ParamConstant.KVSTORE_NAME.getName(), kvStoreName);

        final String kvHelperHostsStr =
            tableProperties.getProperty(ParamConstant.KVSTORE_NODES.getName());

        if (kvHelperHostsStr != null) {
            kvHelperHosts = kvHelperHostsStr.trim().split(",");
        } else {
            throw new IllegalArgumentException
                ("No comma-separated list of hostname:port pairs (KV Helper " +
                 "Hosts) provided via the '" +
                 ParamConstant.KVSTORE_NODES.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        jobProperties.put(
            ParamConstant.KVSTORE_NODES.getName(), kvHelperHostsStr);
        jobConf.set(ParamConstant.KVSTORE_NODES.getName(), kvHelperHostsStr);
        LOG.debug("kvHelperHosts = " + kvHelperHostsStr);

        String kvHadoopHostsStr =
           tableProperties.getProperty(ParamConstant.KVHADOOP_NODES.getName());

        if (kvHadoopHostsStr != null) {
            kvHadoopHosts = kvHadoopHostsStr.trim().split(",");
        } else {
            kvHadoopHosts = new String[kvHelperHosts.length];
            final StringBuilder hadoopBuf = new StringBuilder();
            for (int i = 0; i < kvHelperHosts.length; i++) {
                /* Strip off the ':port' suffix */
                final String[] hostPort = (kvHelperHosts[i]).trim().split(":");
                kvHadoopHosts[i] = hostPort[0];
                if (i != 0) {
                    hadoopBuf.append(",");
                }
                hadoopBuf.append(kvHadoopHosts[i]);
            }
            kvHadoopHostsStr = hadoopBuf.toString();
        }
        jobProperties.put(
            ParamConstant.KVHADOOP_NODES.getName(), kvHadoopHostsStr);
        jobConf.set(ParamConstant.KVHADOOP_NODES.getName(), kvHadoopHostsStr);
        LOG.debug("kvHadoopHosts = " + kvHadoopHostsStr);

        tableName =
            tableProperties.getProperty(ParamConstant.TABLE_NAME.getName());
        if (tableName == null) {
            throw new IllegalArgumentException
                ("No KV Store Table Name provided via the '" +
                 ParamConstant.TABLE_NAME.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        jobProperties.put(ParamConstant.TABLE_NAME.getName(), tableName);
        jobConf.set(ParamConstant.TABLE_NAME.getName(), tableName);
        LOG.debug("tableName = " + tableName);

        primaryKeyProperty = tableProperties.getProperty(
            ParamConstant.PRIMARY_KEY.getName());
        if (primaryKeyProperty != null) {
            jobProperties.put(
                ParamConstant.PRIMARY_KEY.getName(), primaryKeyProperty);
            jobConf.set(
                ParamConstant.PRIMARY_KEY.getName(), primaryKeyProperty);
        }
        LOG.debug("primaryKeyProperty = " + primaryKeyProperty);

        /* For MultiRowOptions. */
        fieldRangeProperty = tableProperties.getProperty(
            ParamConstant.FIELD_RANGE.getName());
        if (fieldRangeProperty != null) {
            jobProperties.put(
                ParamConstant.FIELD_RANGE.getName(), fieldRangeProperty);
            jobConf.set(
                ParamConstant.FIELD_RANGE.getName(), fieldRangeProperty);
        }
        LOG.debug("fieldRangeProperty = " + fieldRangeProperty);

        /*
         * For TableIteratorOptions. Note that when doing PrimaryKey iteration,
         * Direction must be UNORDERED.
         */
        final String consistencyStr = tableProperties.getProperty(
            ParamConstant.CONSISTENCY.getName());
        if (consistencyStr != null) {
            consistency = ExternalDataSourceUtils.parseConsistency(
                                                      consistencyStr);
            jobProperties.put(
                ParamConstant.CONSISTENCY.getName(), consistencyStr);
            jobConf.set(ParamConstant.CONSISTENCY.getName(), consistencyStr);
        }
        LOG.debug("consistency = " + consistencyStr);

        final String timeoutStr = tableProperties.getProperty(
            ParamConstant.TIMEOUT.getName());
        if (timeoutStr != null) {
            timeout = ExternalDataSourceUtils.parseTimeout(timeoutStr);
            timeoutUnit = TimeUnit.MILLISECONDS;
            jobProperties.put(ParamConstant.TIMEOUT.getName(), timeoutStr);
            jobConf.set(ParamConstant.TIMEOUT.getName(), timeoutStr);
        }
        LOG.debug("timeout = " + timeout);

        final String maxRequestsStr = tableProperties.getProperty(
            ParamConstant.MAX_REQUESTS.getName());
        if (maxRequestsStr != null) {
            try {
                maxRequests = Integer.parseInt(maxRequestsStr);
                jobProperties.put(
                    ParamConstant.MAX_REQUESTS.getName(), maxRequestsStr);
                jobConf.set(
                    ParamConstant.MAX_REQUESTS.getName(), maxRequestsStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.MAX_REQUESTS.getName() + " [" +
                     maxRequestsStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.debug("maxRequests = " + maxRequests);

        final String batchSizeStr = tableProperties.getProperty(
            ParamConstant.BATCH_SIZE.getName());
        if (batchSizeStr != null) {
            try {
                batchSize = Integer.parseInt(batchSizeStr);
                jobProperties.put(
                    ParamConstant.BATCH_SIZE.getName(), batchSizeStr);
                jobConf.set(
                    ParamConstant.BATCH_SIZE.getName(), batchSizeStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.BATCH_SIZE.getName() + " [" +
                     batchSizeStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.debug("batchSize = " + batchSize);

        final String maxBatchesStr = tableProperties.getProperty(
            ParamConstant.MAX_BATCHES.getName());
        if (maxBatchesStr != null) {
            try {
                maxBatches = Integer.parseInt(maxBatchesStr);
                jobProperties.put(
                    ParamConstant.MAX_BATCHES.getName(), maxBatchesStr);
                jobConf.set(
                    ParamConstant.MAX_BATCHES.getName(), maxBatchesStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.MAX_BATCHES.getName() + " [" +
                     maxBatchesStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.debug("maxBatches = " + maxBatches);

        kvStoreSecurityFile = tableProperties.getProperty(
            ParamConstant.KVSTORE_SECURITY.getName());
        if (kvStoreSecurityFile != null) {
            jobProperties.put(
                ParamConstant.KVSTORE_SECURITY.getName(), kvStoreSecurityFile);
            jobConf.set(
                ParamConstant.KVSTORE_SECURITY.getName(), kvStoreSecurityFile);
        }
        LOG.debug("kvStoreSecurityFile = " + kvStoreSecurityFile);

        setConf(new Configuration(jobConf));
    }
}
