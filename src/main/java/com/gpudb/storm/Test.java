package com.gpudb.storm;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.RecordObject;
import com.gpudb.protocol.ShowTableRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests streaming data from one table to another via Storm.
 * 
 * GPUdbSpout establishes the streaming data source, using a table monitor and a
 * separate thread for priming the source by ingesting data into it.
 * 
 * GPUdbBolt establishes the streaming data target, inserting records received
 * from the GPUdbSpout into the target table.
 * 
 * The test tables created by this test will be removed upon completion.
 */
public class Test {
    
    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    private static final int DEFAULT_TEST_RECORD_COUNT = 1000000;

    private interface TopologySubmitter {
        void submitTopology(Config config, StormTopology topology) throws Exception;
    }

    /**
     * Data object that will be streamed during Storm test
     */
    public static class TestRecord extends RecordObject {
        /** Single field held by test data object */
        @Column(order = 0)
        public int value;
    }

    private static String runTest(GPUdb gpudb, int testRecordCount, TopologySubmitter submitter) throws Exception {
        
        final String spoutId = "StormTestSpout";
        final String boltId = "StormTestBolt";
        final String sourceTable = "StormTestSource";
        final String targetTable = "StormTestTarget";
        final int testCompletionCheckIntervalSecs = 10;
        final int testCompletionWaitTimeTotalSecs = 600;

        // Clear tables in case they're left over from a previous run

        for (String tableName : Arrays.asList(sourceTable, targetTable))
            if (gpudb.hasTable(tableName, null).getTableExists())
                gpudb.clearTable(targetTable, null, null);

        gpudb.createTable(sourceTable, RecordObject.createType(TestRecord.class, gpudb), null);

        // Create the topology (Spout[sourceTable] -> Bolt[targetTable]), submit
        // it to the cluster, and wait 10 seconds to give it time to spin up.
        // Use a batch count of 9973 (prime number) to ensure it's out of sync
        // with the bulk inserter.

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutId, new GPUdbSpout(gpudb, sourceTable), 10);
        builder.setBolt(boltId, new GPUdbBolt(gpudb, null, targetTable, 9973, 15)).shuffleGrouping(spoutId);
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        submitter.submitTopology(conf, builder.createTopology());
        Utils.sleep(20000);

        // Insert test records using a batch count of 9967 (also prime).

        BulkInserter<TestRecord> bi = new BulkInserter<>(gpudb, sourceTable, RecordObject.getType(TestRecord.class), 9967, null);

        for (int i = 0; i < testRecordCount; i++) {
            TestRecord st = new TestRecord();
            st.value = i;
            bi.insert(st);
        }

        bi.flush();

        // Wait 10 minutes to give the topology time to finish executing, then
        // shut down the cluster.

        for (int waitSecs = 0; waitSecs < testCompletionWaitTimeTotalSecs; waitSecs += testCompletionCheckIntervalSecs)
            if (gpudb.showTable(targetTable, GPUdb.options(ShowTableRequest.Options.GET_SIZES, ShowTableRequest.Options.TRUE)).getTotalSize() < testRecordCount)
                Utils.sleep(testCompletionCheckIntervalSecs * 1000);
            else
                break;

        // Read all the records from targetTable and make sure they match.

        List<TestRecord> newRecords = new ArrayList<>();

        for (int i = 0; i < testRecordCount; i += 10000) {
            newRecords.addAll(gpudb.<TestRecord>getRecords(TestRecord.class, targetTable, i, 10000, null).getData());
        }

        Collections.sort
        (
            newRecords,
            new Comparator<TestRecord>() {
                @Override
                public int compare(TestRecord o1, TestRecord o2) {
                    return o1.value - o2.value;
                }
            }
        );

        for (int i = 0; i < testRecordCount; i++) {
            if (newRecords.get(i).value != i) {
                return "Something went wrong.";
            }
        }

        // Clear tables.

        gpudb.clearTable(sourceTable, null, null);
        gpudb.clearTable(targetTable, null, null);

        return "Everything worked.";
    }

    /**
     * Runs the GPUdb streaming data via Storm test
     * 
     * @param args command line arguments:
     *        {@code --local} - to run against a local Storm cluster
     *        {@code --records=<count>} - total number of test records to stream
     *        {@code --url=<gpudbURL>} - URL of GPUdb instance
     * @throws Exception if something goes awry
     */
    public static void main(String[] args) throws Exception {
        final String topologyName = "StormTestTopology";
        boolean local = false;
        String gpudbUrl = "http://localhost:9191";
        int testRecordCount = DEFAULT_TEST_RECORD_COUNT;

        for (String arg : args) {
            if (arg.equals("--local")) {
                local = true;
            } else if (arg.startsWith("--records=")){
                testRecordCount = Integer.parseInt(arg.substring(10, arg.length()));
            } else if (arg.startsWith("--url=")){
                gpudbUrl = arg.substring(6, arg.length());
            } else {
                System.out.println("Unknown option " + arg);
                return;
            }
        }

        GPUdb gpudb = new GPUdb(gpudbUrl);

        if (local) {
            final LocalCluster cluster = new LocalCluster();

            String result = runTest(gpudb, testRecordCount, new TopologySubmitter() {
                @Override
                public void submitTopology(Config config, StormTopology topology) {
                    cluster.submitTopology(topologyName, config, topology);
                }
            });

            logger.info("Shutting down topology...");

            cluster.killTopology(topologyName);
            cluster.shutdown();
            Utils.sleep(10000);

            logger.info(result);

            // Force exit (since local cluster doesn't seem to shut down its thread
            // pool cleanly).

            System.exit(0);
        } else {
            logger.info(runTest(gpudb, testRecordCount, new TopologySubmitter() {
                @Override
                public void submitTopology(Config config, StormTopology topology) throws Exception {
                    StormSubmitter.submitTopology(topologyName, config, topology);
                }
            }));
        }
    }
}