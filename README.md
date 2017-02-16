# ubispark-api

This is the Ubispark API development repository. The aim is to produce an Android Library that allows opportunistic clusters formation on smart devices and running computation tasks originating from the Internet or one/all of the participating devices. The API should enable running free-form Spark-like automatically distributed code, with data available from HTTP or HTTPS sources.

## Components

The API will be an Android library written in Scala (using Scaloid). It will include:
* A profiler to determine the performance and bandwidth of each node, using parts of ubispark-mobster benchmark
* A cluster formation protocol that chooses nodes based on user preferences, such as energy efficiency, fastest computation time, or a free-form trade-off factor between 0 (favor energy) and 1 (favor performance)
* A scheduler that accepts code and runs computation tasks on a formed cluster or an individual computation node.
* A driver process that communicates tasks to the scheduler and results back to the originator, unless they are already being stored into remote or local storage.
