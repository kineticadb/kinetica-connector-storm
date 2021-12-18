Storm Developer Manual
======================

The following guide provides step by step instructions to get started
integrating *Kinetica* with *Storm*.

This project is aimed to make *Kinetica* accessible via *Storm*, meaning data
can be streamed from a *Kinetica* table or to a *Kinetica* table.  The custom
*Storm Spout* and *Bolt* do no additional processing.

Source code for the connector can be found at
https://github.com/kineticadb/kinetica-connector-storm


Connector Classes
-----------------

The two connector classes that integrate *Kinetica* with *Storm* are:

``com.gpudb.storm``

* ``GPUdbSpout`` - A *Storm Spout*, which receives a data stream from a *Kinetica*
  database table monitor
* ``GPUdbBolt`` - A *Storm Bolt*, which receives a data stream from a *Spout*
  and writes it to *Kinetica*

-----


Streaming Data from Kinetica into a Storm Spout
-----------------------------------------------

A ``GPUdbSpout`` needs to be instantiated with the *Kinetica* source connection
information and the name of the table from which data will be streamed::

        GPUdbSpout gpudbSpout = new GPUdbSpout(gpudbConn, sourceTableName);

When this *Spout* is used as part of a topology, it will emit streamed records
for processing by any configured *Bolts*.


-----


Streaming Data from a Storm Bolt to Kinetica
--------------------------------------------

A ``GPUdbBolt`` needs to be instantiated with the *Kinetica* target connection
information, the name of the collection & table to which data will be streamed,
and the size & time thresholds that should be used to trigger data flushes::

        GPUdbBolt gpudbBolt = new GPUdbBolt(gpudbConn, targetCollectionName, targetTableName, batchSizeLimit, timeLimitSecs);

The *Bolt* will create the target table & collection, if either do not exist.

If using multi-head ingestion, the *Bolt* can be created with the prefix of the
*Kinetica* node IP addresses:: 

        GPUdbBolt gpudbBolt = new GPUdbBolt(gpudbConn, targetCollectionName, targetTableName, batchSizeLimit, timeLimitSecs, ipPrefix);


-----


Connecting the Streaming Spout to the Streaming Bolt
----------------------------------------------------

To connect the ``GPUdbSpout`` to the ``GPUdbBolt``, they need to be added to a
configured *Storm* topology and connected::

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutId, gpudbSpout, parallelismHint);
        builder.setBolt(boltId, gpudbBolt).shuffleGrouping(spoutId);
        Config config = new Config();
        config.setNumWorkers(numWorkers);

They can then be submitted to *Storm*, either by using a simulated cluster::

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, config, builder.createTopology());

or by using a live *Storm* cluster::

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());


-----


Examples
--------

An example can be found in the ``com.gpudb.storm`` package:

* ``Test`` - Streaming data from *Kinetica* to *Kinetica* via *Storm* using a
  custom *Spout* & *Bolt*


-----


Installation & Configuration
----------------------------

The example code provided in this project can be run in local mode, where the
*Storm* environment is simulated, or on a live *Storm* cluster.  In local mode,
the example can be run on any server.  In cluster mode, the example must be run
on the head *Storm* node.

Two JAR files are produced by this project:

* ``storm-connector-<ver>.jar`` - can only be run in cluster mode
* ``storm-connector-<ver>-jar-with-dependencies.jar`` - can be run in local or
  cluster mode

To run the example, issue the *Unix* command::

        java -jar <kineticaStormJar> [--local] [--records=<RECORDS>] [--url=<URL>] [--ipPrefix=<IPPREFIX>] [--user=<USER>] [--pass=<PASS>]

where::

            kineticaStormJar - path to JAR containing Kinetica Storm example
            --local          - (optional) if specified, process will run in Storm
                               simulator; if not, will run in Storm cluster
            --records        - (optional) if specified, <RECORDS> will be the
                               total number of records processed; default 1,000,000
            --url            - (optional) if specified, <URL> will be the URL
                               of the Kinetica instance; default http://localhost:9191
            --ipPrefix       - (optional) if specified, <IPPREFIX> will be the
                               prefix of the IP addresses of the Kinetica nodes
                               targeted for multi-head ingestion; useful, if
                               those nodes have multiple IP addresses
            --user           - (optional) if specified, <USER> will be the user name
                               used to authenticate to the Kinetica instance
            --pass           - (optional) if specified, <PASS> will be the password
                               used to authenticate to the Kinetica instance

example::

        java -jar storm-connector-1.0-jar-with-dependencies.jar --local --records=100000 --url=http://localhost:9191 --ipPrefix=172.30
