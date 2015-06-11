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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.hadoop.table.TableInputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Concrete implementation of the InputSplit interface required by version 1
 * of MapReduce to support Hive queries. A RecordReader will take instances
 * of this class, where each such instance corresponds to data stored in
 * an Oracle NoSQL Database store via the Table API, and use those instances
 * to retrieve that data when performing a given Hive query against the
 * store's data.
 * <p>
 * Note that the Hive infrastructure requires that even though the data
 * associated with instances of this class resides in a table in an Oracle
 * NoSQL Database store rather than an HDFS file, this class still must
 * subclass FileSplit. As a result, a Hadoop HDFS Path must be specified
 * for this class.
 * <p>
 * Also note that although this InputSplit is based on version 1 of MapReduce
 * (as requied by the Hive infrastructure), it wraps and delegates to a YARN
 * based (MapReduce version 2) InputSplit. This is done because the InputSplit
 * class Oracle NoSQL Database provides to support Hadoop integration is YARN
 * based, and this class wishes to exploit and reuse the functionality already
 * provided by the YARN based InputSplit class.
 */
public class TableHiveInputSplit extends FileSplit {

    private final TableInputSplit v2Split;

    private static final String[] EMPTY_STRING_ARRAY = new String[] { };

    public TableHiveInputSplit() {
        super((Path) null, 0, 0, EMPTY_STRING_ARRAY);
        this.v2Split = new TableInputSplit();
    }

    public TableHiveInputSplit(Path filePath, TableInputSplit v2Split) {
        super(filePath, 0, 0, EMPTY_STRING_ARRAY);
        this.v2Split = v2Split;
    }

    /**
     * Returns the HDFS Path associated with this split.
     *
     * @return the HDFS Path associated with this split
     */
    @Override
    public Path getPath() {
        return super.getPath();
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     *
     * @return the number of bytes in the split
     */
    @Override
    public long getLength() {

        return v2Split.getLength();
    }

    /**
     * Get the list of nodes by name where the data for the split would be
     * local.  The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     * @throws IOException
     */
    @Override
    public String[] getLocations() throws IOException {

        return v2Split.getLocations();
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        v2Split.write(out);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in
     * the existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        v2Split.readFields(in);
    }

    /*
     * A well-defined equals, hashCode, and toString method must be provided
     * so that instances of this class can be compared, uniquely identified,
     * and stored in collections.
     */

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableHiveInputSplit)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final TableHiveInputSplit obj1 = this;
        final TableHiveInputSplit obj2 = (TableHiveInputSplit) obj;

        final Path path1 = obj1.getPath();
        final Path path2 = obj2.getPath();

        if (path1 != null) {
            if (!path1.equals(path2)) {
                return false;
            }
        } else {
            if (path2 != null) {
                return false;
            }
        }
        return obj1.v2Split.equals(obj2.v2Split);
    }

    @Override
    public int hashCode() {

        int hc = 0;
        final Path filePath = getPath();
        if (filePath != null) {
            hc = filePath.hashCode();
        }

        return hc + v2Split.hashCode();
    }

    @Override
    public String toString() {
        if (v2Split == null) {
            return super.toString();
        }
        final StringBuilder buf =
            new StringBuilder(this.getClass().getSimpleName());
        buf.append(": [path=");
        buf.append(getPath());
        buf.append("], ");
        buf.append(v2Split.toString());

        return buf.toString();
    }

    public String getKVStoreName() {
        return v2Split.getKVStoreName();
    }

    public String[] getKVHelperHosts() {
        return v2Split.getKVHelperHosts();
    }

    public String getTableName() {
        return v2Split.getTableName();
    }
}
