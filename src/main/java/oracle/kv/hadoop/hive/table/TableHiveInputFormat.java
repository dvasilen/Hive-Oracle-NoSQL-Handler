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
import java.util.Set;

import oracle.kv.hadoop.table.TableInputSplit;
import oracle.kv.hadoop.table.TableRecordReader;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * A Hadoop MapReduce version 1 InputFormat class for reading data from an
 * Oracle NoSQL Database when processing a Hive query against data written
 * to that database using the Table API.
 * <p>
 * Note that whereas this class is an instance of a version 1 InputFormat
 * class, in order to exploit and reuse the mechanisms provided by the
 * Hadoop integration classes (in package oracle.kv.hadoop.table), this class
 * also creates and manages an instance of a version 2 InputFormat.
 */
public class TableHiveInputFormat<K, V>
                 implements org.apache.hadoop.mapred.InputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(
                   "oracle.kv.hadoop.hive.TableHiveInputFormat");

    /**
     * Returns the RecordReader for the given InputSplit.
     * <p>
     * Note that the RecordReader that is returned is based on version 1 of
     * MapReduce, but wraps and delegates to a YARN based (MapReduce version2)
     * RecordReader. This is done because the RecordReader provided for
     * Hadoop integration is YARN based, whereas the Hive infrastructure
     * requires a version 1 RecordReader.
     */
    @Override
    @SuppressWarnings("unchecked")
    public RecordReader<K, V> getRecordReader(InputSplit split,
                                              JobConf job,
                                              Reporter reporter)
        throws IOException {

        LOG.debug("split = " + split);

        final TableInputSplit v2Split = V1V2TableUtil.getSplitMap(
                                  job, (TableHiveInputSplit) split).get(split);

        final TableRecordReader v2Reader = new TableRecordReader();
        try {
            v2Reader.initialize(v2Split, new TableTaskAttemptContext(job));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        /*
         * Must perform an unchecked cast; otherwise an eclipse warning about
         * RecordReader being a raw type that needs to be parameterized will
         * occur. But an eclipse unchecked warning cannot be avoided because
         * of type erasure; so suppress unchecked warnings in this method.
         */
        return (RecordReader<K, V>) (new TableHiveRecordReader(v2Reader));
    }

    /**
     * Returns an array containing the input splits for the given job.
     * <p>
     * Implementation Note: when V1V2TableUtil.getInputFormat() is called by
     * this method to retrieve the TableInputFormat instance to use for a given
     * query, only the VERY FIRST call to V1V2TableUtil.getInputFormat() (after
     * the query has been entered on the command line and the input info for
     * the job has been reset) will construct an instance of TableInputFormat;
     * all additional calls -- while that query is executing -- will always
     * return the original instance created by that first call. Note also
     * that in addition to constructing a TableInputFormat instance, that
     * first call to V1V2TableUtil.getInputFormat() also populates the
     * splitMap; which is achieved via a call to getSplits() on the newly
     * created TableInputFormat instance.
     *
     * Since the first call to V1V2TableUtil.getInputFormat() has already
     * called TableInputFormat.getSplits() and placed the retrieved splits
     * in the splitMap, it is no longer necessary to make any additional
     * calls to TableInputFormat.getSplits(). Not only is it not necessary to
     * call TableInputFormat.getSplits(), but such a call should be avoided.
     * This is because any call to TableInputFormat.getSplits() will result
     * in remote calls to the KVStore; which can be very costly.
     *
     * Thus, one should NEVER make a call such as,
     * V1V2TableUtil.getInputFormat().getSplits(); as such a call may result
     * in two successive calls to TableInputFormat.getSplits(). To avoid
     * this situation, one should employ a two step process like the
     * following to retrieve and return the desired splits:
     *
     * 1. First call V1V2TableUtil.getInputFormat(); which when called
     *    repeatedly, will always return the same instance of
     *    TableInputFormat.
     * 2. Call V1V2TableUtil.getSplitMap(), then retrieve and return the
     *    desired splits from the returned map.
     */
    @Override
    @SuppressWarnings("unused")
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {

        final InputFormat<PrimaryKey, Row> v2InputFormat =
                                           V1V2TableUtil.getInputFormat(job);

        final Set<TableHiveInputSplit> v1SplitKeySet =
            V1V2TableUtil.getSplitMap(job).keySet();

        return v1SplitKeySet.toArray(
                   new TableHiveInputSplit[v1SplitKeySet.size()]);
    }
}
