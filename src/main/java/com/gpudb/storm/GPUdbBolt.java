package com.gpudb.storm;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.protocol.CreateTableRequest;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm Bolt for ingesting data into a GPUdb table.
 * 
 * If the ingestion target table does not exist, it will be created and given a
 * schema that matches the type of the data being ingested.  Subsequently, all
 * data of that type should be ingested successfully into the new table.
 * 
 * If the ingestion target table does exist, the schema will be extracted from
 * it and compared will all inbound data, field name to field name.  If an
 * inbound record's fields contain a match, in name and type, for each of the
 * columns in the target table, the values in the record's matching fields will
 * be ingested successfully into the table.  Any fields contained within the
 * inbound record that do not match the names of any columns in the target table
 * will be dropped.  If an inbound record's fields do not contain a match, in
 * name and type, for one or more of the target table's columns, the entire
 * record will be dropped.
 * 
 * The Bolt will be configured for both a size-based and time-based insertion
 * flush threshold to improve overall throughput with both high-volume and
 * high-latency data flows.
 */
public class GPUdbBolt implements IRichBolt {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GPUdbBolt.class);

    private final URL url;
    private final String username;
    private final String password;
    private final int timeout;
    private final String collectionName;
    private final String tableName;
    private final int batchSize;
    private final int flushInterval;
    private final String ipPrefix;
    private transient GPUdb gpudb;
    private transient BulkInserter<GenericRecord> bi;
    private transient Type type;
    private transient List<Tuple> tuples;
    private transient OutputCollector collector;

    /**
     * Constructs a Storm Bolt for ingesting data into a GPUdb table
     * 
     * @param gpudb a configured GPUdb connection, the settings of which will be
     *        used to make data ingestion connections to GPUdb
     * @param collectionName name of the collection in which the target table
     *        exists or should be created if not existent; if null, the target
     *        table will be top-level (not part of a collection)
     * @param tableName name of the target table into which data will be
     *        ingested; will be created, if not existent
     * @param batchSize number of records to accumulate before ingesting them as
     *        a group
     * @param flushInterval interval, in seconds, at which accumulated data
     *        should be ingested, if batchSize is not reached during that time
     *        frame
     */
    public GPUdbBolt(GPUdb gpudb, String collectionName, String tableName, int batchSize, int flushInterval) {
        this(gpudb, collectionName, tableName, batchSize, flushInterval, null);
    }

    /**
     * Constructs a Storm Bolt for ingesting data into a GPUdb table
     * 
     * @param gpudb a configured GPUdb connection, the settings of which will be
     *        used to make data ingestion connections to GPUdb
     * @param collectionName name of the collection in which the target table
     *        exists or should be created if not existent; if null, the target
     *        table will be top-level (not part of a collection)
     * @param tableName name of the target table into which data will be
     *        ingested; will be created, if not existent
     * @param batchSize number of records to accumulate before ingesting them as
     *        a group
     * @param flushInterval interval, in seconds, at which accumulated data
     *        should be ingested, if batchSize is not reached during that time
     *        frame
     * @param ipPrefix prefix of IP addresses to use for multi-head ingest
     */
    public GPUdbBolt(GPUdb gpudb, String collectionName, String tableName, int batchSize, int flushInterval, String ipPrefix) {
        // Save the GPUdb parameters from the specified API instance so that a
        // new instance can be created later. This is necessary because the
        // GPUdb object is not serializable and the bolt may be run on a
        // different machine than its constructor.

        url = gpudb.getURL();
        username = gpudb.getUsername();
        password = gpudb.getPassword();
        timeout = gpudb.getTimeout();
        this.collectionName = collectionName == null ? "" : collectionName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.ipPrefix = ipPrefix;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // Limit the bolt to a single thread, and request ticks to ensure that
        // records are flushed every so often (for low-frequency sources).

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);

        if (batchSize > 1) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, flushInterval);
        }

        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // The bolt outputs nothing, so no output fields are needed.
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            gpudb = new GPUdb(url, new GPUdb.Options().setUsername(username).setPassword(password).setTimeout(timeout));
        } catch (GPUdbException ex) {
            LOG.error("Unable to connect to GPUdb at " + url + ": " + ex.getMessage(), ex);
        }

        tuples = new ArrayList<>(batchSize);
        this.collector = collector;
    }

    private void failAllTuples() {
        // Loop through all tuples in the queue and fail each one, then clear
        // the queue.

        for (Tuple tuple : tuples) {
            collector.fail(tuple);
        }

        tuples.clear();
    }

    private void flush() {
        if (tuples.isEmpty()) {
            return;
        }

        // If there is no schema yet (because this is the first time flush was
        // called), either look it up from the specified table (if it exists)
        // or create one (and the table) by analyzing the first tuple in the
        // queue.

        if (type == null) {
            // If the table exists, get the schema from it.

            try {
                if (gpudb.hasTable(tableName, null).getTableExists()) {
                    type = Type.fromTable(gpudb, tableName);
                }
            } catch (Exception ex) {
                LOG.error("Unable to lookup table " + tableName + " in GPUdb at " + url + ": " + ex.getMessage(), ex);
                failAllTuples();
                return;
            }

            // If the table does not exist, loop through the first tuple's
            // fields and build a type based on their names and types.

            if (type == null) {
                Tuple tuple = tuples.get(0);
                int fieldCount = tuple.getFields().size();
                List<Type.Column> columns = new ArrayList<>(fieldCount);

                for (int i = 0; i < fieldCount; i++) {
                    Class<?> columnType;
                    Object value = tuple.getValue(i);

                    if (value instanceof byte[]) {
                        columnType = ByteBuffer.class;
                    } else if (value instanceof Double) {
                        columnType = Double.class;
                    } else if (value instanceof Float) {
                        columnType = Float.class;
                    } else if (value instanceof Integer) {
                        columnType = Integer.class;
                    } else if (value instanceof Long) {
                        columnType = Long.class;
                    } else if (value instanceof String) {
                        columnType = String.class;
                    } else {
                        if (value == null) {
                            LOG.error("Unsupported null value in field " + tuple.getFields().get(i) + ".");
                        } else {
                            LOG.error("Unsupported type " + value.getClass().getName() + " in field " + tuple.getFields().get(i) + ".");
                        }

                        collector.fail(tuple);
                        tuples.remove(0);
                        return;
                    }

                    columns.add(new Type.Column(tuple.getFields().get(i), columnType));
                }

                // Create the table based on the newly created schema.

                try {
                    type = new Type(columns);
                    gpudb.createTable(tableName, type.create(gpudb),
                            GPUdb.options(CreateTableRequest.Options.COLLECTION_NAME, collectionName));
                } catch (Exception ex) {
                    LOG.error("Unable to create table " + tableName + " in GPUdb at " + url + ": " + ex.getMessage(), ex);
                    failAllTuples();
                    return;
                }
            }
        }

        List<Tuple> acks = new ArrayList<>(tuples.size());
        List<Tuple> fails = new ArrayList<>();
        List<GenericRecord> data = new ArrayList<>(tuples.size());

        // Loop through all tuples in the queue and create records out of them.

        for (Tuple tuple : tuples) {
            GenericRecord record = new GenericRecord(type);
            Fields fields = tuple.getFields();
            boolean fail = false;

            // Loop through all fields in the tuple.

            for (int i = 0; i < fields.size(); i++) {
                int columnIndex = type.getColumnIndex(fields.get(i));
                Type.Column column = type.getColumn(columnIndex);

                // If the field is not in the schema, ignore it.

                if (column != null) {
                    // Check the type of the field in the tuple versus the
                    // type of the field in the schema to ensure compatibility;
                    // if not compatible, fail the tuple, otherwise copy the
                    // value.

                    Object value = tuple.getValue(i);

                    if (value == null) {
                        LOG.error("Unsupported null value in field " + fields.get(i) + ".");
                        fail = true;
                        break;
                    }

                    Class<?> columnType = column.getType();

                    if (columnType == ByteBuffer.class) {
                        if (value instanceof byte[]) {
                            value = ByteBuffer.wrap((byte[])value);
                        } else {
                            fail = true;
                        }
                    } else if (columnType == Double.class) {
                        if (!(value instanceof Double)) {
                            fail = true;
                        }
                    } else if (columnType == Float.class) {
                        if (!(value instanceof Float)) {
                            fail = true;
                        }
                    } else if (columnType == Integer.class) {
                        if (!(value instanceof Integer)) {
                            fail = true;
                        }
                    } else if (columnType == Long.class) {
                        if (!(value instanceof Long)) {
                            fail = true;
                        }
                    } else if (columnType == String.class) {
                        if (!(value instanceof String)) {
                            fail = true;
                        }
                    }

                    if (fail) {
                        LOG.error("Type mismatch in field " + fields.get(i) + ".");
                        break;
                    }

                    record.put(columnIndex, value);
                }
            }

            // If the tuple did not fail, check to make sure all fields in the
            // schema are accounted for, and if not, fail the tuple.

            if (!fail) {
                for (int i = 0; i < type.getColumnCount(); i++) {
                    if (record.get(i) == null) {
                        LOG.error("Missing field " + type.getColumn(i).getName() + ".");
                        fail = true;
                        break;
                    }
                }
            }

            // Add the tuple (and, if it didn't fail, the created record) to the
            // appropriate lists for processing.

            if (fail) {
                fails.add(tuple);
            } else {
                acks.add(tuple);
                data.add(record);
            }
        }

        // Insert records for any tuples that didn't fail into GPUdb.

        try {
            if (bi == null)
                bi = new BulkInserter<>(gpudb, tableName, type, batchSize, null, (ipPrefix == null) ? null : new BulkInserter.WorkerList(gpudb, ipPrefix));
            bi.insert(data);
            bi.flush();
        } catch (Exception ex) {
            LOG.error("Unable to insert into table " + tableName + " in GPUdb at " + url + ": " + ex.getMessage(), ex);
            failAllTuples();
            return;
        }

        // Acknowledge and fail each tuple as appropriate.

        for (Tuple tuple : acks) {
            collector.ack(tuple);
        }

        for (Tuple tuple : fails) {
            collector.fail(tuple);
        }

        // Clear the tuple queue.

        tuples.clear();
    }

    @Override
    public void execute(Tuple tuple) {
        // If the received tuple is a tick, call flush to empty out the queue.
        // Otherwise, add the tuple to the queue, and if the batch size has
        // been reached, call flush.

        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            flush();
        } else {
            tuples.add(tuple);

            if (tuples.size() >= batchSize) {
                flush();
            }
        }
    }

    @Override
    public void cleanup() {
        flush();
    }
}