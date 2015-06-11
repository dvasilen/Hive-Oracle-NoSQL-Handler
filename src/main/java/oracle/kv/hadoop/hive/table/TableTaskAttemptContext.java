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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Concrete implementation of Hadoop TaskAttemptContext interface. When
 * initializing a YARN based (MapReduce version 2) RecordReader, an
 * instance of TaskAttemptContext must be specified. This class is intended
 * to satisfy that requirement.
 *
 * Note that the RecordReader provided for Hadoop integration is YARN based,
 * whereas the Hive infrastructure requires a RecordReader based on version 1
 * MapReduce. To address this incompatibility when integrating with HIVE,
 * a version 1 based RecordReader is used, but wraps and delegates to a
 * version 2 RecordReader; thus, the need for this class. Currently, the
 * methods of this class are "stubbed out" and/or return default values;
 * which seems to be acceptable for the Hive integration.
 */
public class TableTaskAttemptContext implements TaskAttemptContext {

    private Job job;
    private String currentStatus = "UNKNOWN";

    @SuppressWarnings("deprecation")
    public TableTaskAttemptContext(Configuration jobConf) throws IOException {
        this.job = new Job(jobConf);
    }

    /*
     * Specified by TaskAttemptContext interface.
     */
    @Override
    public Counter getCounter(Enum<?> counterName) {
        return null;
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
        return null;
    }

    @Override
    public float getProgress() {
        return 0.0f;
    }

    @Override
    public String getStatus() {
        return currentStatus;
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        return null;
    }

    @Override
    public void setStatus(String status) {
        currentStatus = status;
    }

    /**
     * Specified by <code>Progressable</code> interface
     * (<code>TaskAttemptContext</code> extends <code>Progressable</code>).
     */
    @Override
    public void progress() {
        /* No-op */
    }

    /**
     * Specified by <code>JobContext</code> interface
     * (<code>TaskAttemptContext</code> extends <code>JobContext</code>).
     *
     * Note that whereas it would be preferrable to define this class
     * to implement <code>TaskAttemptContext</code> and extend the
     * <code>Job</code> class (so that this class inherits the
     * implementations of all of the methods defined below), that cannot
     * be done. This is because the <code>Job</code> class implements
     * the method <code>JobStatus getStatus</code>, while the
     * <code>TaskAttemptContext</code> interface specifies the method
     * <code>String getStatus</code>. As a result, a sub-class of
     * <code>Job</code> cannot implement <code>TaskAttemptContext</code>;
     * because a method implementation that satisfies the
     * <code>getStatus</code> specification in <code>TaskAttemptContext</code>
     * cannot override the implementation of <code>getStatus</code>
     * provided by <code>Job</code>.
     */
    @Override
    public Path[] getArchiveClassPaths() {
        return job.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
        return job.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        return job.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        return job.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
        return job.getCombinerClass();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        return job.getCombinerKeyGroupingComparator();
    }

    @Override
    public Configuration getConfiguration() {
        return job.getConfiguration();
    }

    @Override
    public org.apache.hadoop.security.Credentials getCredentials() {
        return job.getCredentials();
    }

    @Override
    public Path[] getFileClassPaths() {
        return job.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
        return job.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        return job.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
        return job.getInputFormatClass();
    }

    @Override
    public String getJar() {
        return job.getJar();
    }

    @Override
    public JobID getJobID() {
        return job.getJobID();
    }

    @Override
    public String getJobName() {
        return job.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        return job.getJobSetupCleanupNeeded();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        return job.getLocalCacheArchives();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        return job.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        return job.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        return job.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
        return job.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
        return job.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
        return job.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
        return job.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
        return job.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return job.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
        return job.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
        return job.getPartitionerClass();
    }

    @Override
    public boolean getProfileEnabled() {
        return job.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
        return job.getProfileParams();
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        return job.getProfileTaskRange(isMap);
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
        return job.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
        return job.getSortComparator();
    }

    @Override
    public boolean getSymlink() {
        return job.getSymlink();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
        return job.getTaskCleanupNeeded();
    }

    @Override
    public String getUser() {
        return job.getUser();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        return job.getWorkingDirectory();
    }

/*
    @Override
    public boolean userClassesTakesPrecedence() {
        return job.userClassesTakesPrecedence();
    }
*/	
}
