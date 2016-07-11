package com.gpudb.storm;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import com.gpudb.Avro;
import com.gpudb.GenericRecord;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Storm Spout for streaming data from a GPUdb table.
 * 
 * The data streaming pipeline will begin with creating a table monitor on the
 * given source table.  As records are inserted into the table, a copy will be
 * placed on a queue.  The Spout will connect to that queue and stream records
 * from it.
 * 
 * The streaming source table can either be part of a collection or not, but
 * cannot be a collection itself.
 */
public class GPUdbSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GPUdbSpout.class);

    private final URL url;
    private final String username;
    private final String password;
    private final String tableName;
    private final String schemaString;
    private final int timeout;
    private int zmqPort;
    private transient ConcurrentLinkedQueue<Values> queue;
    private transient Thread monitorThread;
    private transient volatile boolean active;
    private transient SpoutOutputCollector collector;

    /**
     * Constructs a Storm Bolt for streaming data from a GPUdb table
     * 
     * @param gpudb a configured GPUdb connection, the settings of which will be
     *        used to make a data streaming connections to GPUdb
     * @param tableName name of the source table from which data will be
     *        extracted; must not be the name of a collection
     * @throws GPUdbException if a Spout connection is unable to be configured
     *         properly to connect to a GPUdb table monitor, if the specified
     *         table is a collection, or if a connection to GPUdb cannot be made
     */
    public GPUdbSpout(GPUdb gpudb, String tableName) throws GPUdbException {
        // Save the GPUdb parameters from the specified API instance so that a
        // new instance can be created later. This is necessary because the
        // GPUdb object is not serializable and the spout may be run on a
        // different machine than its constructor.

        url = gpudb.getURL();
        username = gpudb.getUsername();
        password = gpudb.getPassword();
        timeout = gpudb.getTimeout();
        this.tableName = tableName;

        // Get table info from /show/table.

        ShowTableResponse tableInfo = gpudb.showTable(tableName, GPUdb.options(ShowTableRequest.Options.SHOW_CHILDREN, ShowTableRequest.Options.FALSE));

        // If the specified table is a collection, fail.

        if (tableInfo.getTableDescriptions().get(0).contains(ShowTableResponse.TableDescriptions.COLLECTION)) {
            throw new GPUdbException("Cannot create spout for collection " + tableName + ".");
        }

        // Get the table schema for decoding records.

        schemaString = tableInfo.getTypeSchemas().get(0);

        // Get the table monitor port from /show/system/properties. If table
        // monitor support is not enabled or the port is invalid, fail.

        String zmqPortString = gpudb.showSystemProperties(GPUdb.options()).getPropertyMap().get("conf.set_monitor_port");

        if (zmqPortString == null || zmqPortString.equals("-1")) {
            throw new GPUdbException("Table monitor not supported.");
        }

        try {
            zmqPort = Integer.parseInt(zmqPortString);
        } catch (Exception ex) {
            throw new GPUdbException("Invalid object monitor port (" + zmqPortString + ").");
        }

        if (zmqPort < 1 || zmqPort > 65535) {
            throw new GPUdbException("Invalid object monitor port (" + zmqPortString + ").");
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // Limit the spout to a single thread.

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare a field for each column in the table schema.

        List<String> fieldNames = new ArrayList<>();

        for (Type.Column column : new Type(schemaString).getColumns()) {
            fieldNames.add(column.getName());
        }

        declarer.declare(new Fields(fieldNames));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new ConcurrentLinkedQueue<>();

        // Create a thread that will manage the table monitor and convert
        // records from the monitor into tuples and put them into the tuple
        // queue.

        monitorThread = new Thread() {
            @Override
            public void run() {
                GPUdb gpudb;
                String topicId;
                Type type;

                // Create the table monitor.

                try {
                    gpudb = new GPUdb(url.toString(), new GPUdb.Options().setUsername(username).setPassword(password).setTimeout(timeout));
                    topicId = gpudb.createTableMonitor(tableName, null).getTopicId();
                    type = new Type(schemaString);
                } catch (GPUdbException ex) {
                    LOG.error("Could not create table monitor for " + tableName + " at " + url + ".", ex);
                    return;
                }

                // Subscribe to the ZMQ topic for the table monitor.

                String zmqUrl = "tcp://" + url.getHost() + ":" + zmqPort;

                try (ZMQ.Context zmqContext = ZMQ.context(1); ZMQ.Socket subscriber = zmqContext.socket(ZMQ.SUB)) {
                    subscriber.connect(zmqUrl);
                    subscriber.subscribe(topicId.getBytes());
                    subscriber.setReceiveTimeOut(1000);

                    // Loop until the thread is interrupted when the spout is
                    // closed.

                    while (!Thread.currentThread().isInterrupted()) {
                        // Check for a ZMQ message; if none was received within
                        // the timeout window, or the spout is not active,
                        // continue waiting.

                        ZMsg message = ZMsg.recvMsg(subscriber);

                        if (message == null || !active) {
                            continue;
                        }

                        boolean skip = true;

                        for (ZFrame frame : message) {
                            // Skip the first frame (which just contains the
                            // topic ID).

                            if (skip) {
                                skip = false;
                                continue;
                            }

                            // Create a tuple for each record and put it into
                            // the tuple queue.

                            GenericRecord object = Avro.decode(type, ByteBuffer.wrap(frame.getData()));
                            Values tuple = new Values();

                            for (int i = 0; i < type.getColumnCount(); i++) {
                                tuple.add(object.get(i));
                            }

                            queue.add(tuple);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Could not access table monitor for " + tableName + " at " + zmqUrl + ".", ex);
                }

                // The spout has been closed (or something failed) so clear the
                // table monitor.

                try {
                    gpudb.clearTableMonitor(topicId, null);
                } catch (GPUdbException ex) {
                    LOG.error("Could not clear table monitor for " + tableName + " at " + url + ".", ex);
                }
            }
        };

        monitorThread.start();
        this.collector = collector;
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void nextTuple() {
        // Check for a tuple in the tuple queue; if not found, sleep for a bit
        // and return control to Storm; otherwise, emit the tuple and continue
        // checking until the queue is empty.

        Values tuple = queue.poll();

        if (tuple == null) {
            Utils.sleep(1);
            return;
        }

        while (tuple != null) {
            collector.emit(tuple);
            tuple = queue.poll();
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void deactivate() {
        active = false;
    }

    @Override
    public void close() {
        // Interrupt the monitor thread and wait for it to terminate.

        monitorThread.interrupt();

        try {
            monitorThread.join();
        } catch (Exception ex) {
        }
    }
}