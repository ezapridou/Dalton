/*Copyright (c) 2022 Data Intensive Applications and Systems Laboratory (DIAS)
                   Ecole Polytechnique Federale de Lausanne
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioning.*;
import partitioning.dalton.DaltonPartitioner;
import partitioning.prompt.Prompt;

public class MyWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(MyWordCount.class);

    public static void buildTopology(String topoName, String partitionerStr, int mappers, String file) throws Exception{
        int batchSize = 1000;
        int windowSize = 60000;
        int windowSlide = 1000;
        int numOfKeys = 100000;

        Config conf = new Config();
        conf.setNumAckers(1);
        conf.setNumWorkers(5);

        int numOfSpouts = 3; // increase until the maximum throughput is reached
        MySpout[] spouts = new MySpout[numOfSpouts];
        for (int i=0; i<numOfSpouts; i++){
            spouts[i] = new MySpout(new Fields("key", "tupleString"), batchSize, LOG, file, i, numOfSpouts);
            spouts[i].setCycle(true);
        }

        TridentTopology topology = new TridentTopology();

        Stream[] streams = new Stream[numOfSpouts];
        for (int i=0; i<numOfSpouts; i++){
            streams[i] = topology.newStream("spout" + i, spouts[i]).parallelismHint(1).name("src" + i);
        }

        BaseAggregator<Integer> partitioner = initPartitioner(partitionerStr, mappers, windowSlide, windowSize, numOfKeys, batchSize);

        topology.merge(streams[0], streams[1], streams[2])
                .parallelismHint(mappers).name("aftersource")
                .aggregate(new Fields("key", "tupleString"), partitioner, new Fields("key", "tupleString", "partitionKey")).parallelismHint(1).name("afterpartitioning")
                // group by
                .partitionBy(new Fields("partitionKey")).parallelismHint(mappers)
                .partitionAggregate(new Fields("key", "tupleString"), new MapAggregator(), new Fields("counts")).parallelismHint(mappers).toStream().name("afterMap")
                .window(SlidingCountWindow.of(mappers*(windowSize/windowSlide), mappers), new InMemoryWindowsStoreFactory(), new Fields("counts"), new ReduceAggregator(), new Fields("key", "word", "count")).parallelismHint(1).name("afterReduce");


        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topology.build());
    }

    private static BaseAggregator<Integer> initPartitioner(String partitionerStr, int mappers, int windowSlide, int windowSize, int numOfKeys, int batchSize){
        if (partitionerStr.equals("Hash")){
            return new HashPartitioner(mappers);
        }
        if (partitionerStr.equals("Shuffle")){
            return new ShufflePartitioner(mappers);
        }
        if (partitionerStr.equals("Dalton")){
            return new DaltonPartitioner(mappers , windowSlide, windowSize, numOfKeys, batchSize);
        }
        if (partitionerStr.equals("cAM")){
            return new cAM(mappers);
        }
        if (partitionerStr.equals("CM")){
            return new CM(mappers);
        }
        if (partitionerStr.equals("TwoChoices")){
            return new TwoChoices(mappers);
        }
        if (partitionerStr.equals("Prompt")){
            return new Prompt(mappers, batchSize);
        }
        throw new IllegalArgumentException("Wrong partitioner name");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 4) {
            String topoName = args[0];
            String partitionerStr = args[1];
            int mappers = Integer.parseInt(args[2]);
            buildTopology(topoName, partitionerStr, mappers, args[3]);
        }
        else{
            LOG.info("Wrong number of attributes. Please give the topology name, the name of the partiitoner to be used," +
                    " the number of workers and the dataset file");
        }
    }
}
