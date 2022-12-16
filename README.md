# Dalton
We implemented our algorithm and the baselines in Flink v1.12 and Storm Trident v2.4.0 using Java 11.0.9.1. 

The only modification to the source code of Flink we did was to modify the function assigning a key to a key-group index (org.apache.flink.runtime.state.KeyGroupRangeAssignment) so that instead of MurmurHash it uses a simple hashFunction: keyHash % maxParallelism. 
