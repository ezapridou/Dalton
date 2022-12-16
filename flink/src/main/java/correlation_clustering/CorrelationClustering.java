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

package correlation_clustering;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.IllegalArgumentException;
import java.util.*;

import record.*;
import correlation_clustering.containers.PartialClusteringOutput;
import partitioning.*;
import sources.*;
import static helperfunctions.PartitionerAssigner.initializePartitioner;

enum CORR_CLUSTERING {
    BEST,
    VOTE
}

/**
 * Correlation Clustering for Streaming data
 * <p>
 */

public class CorrelationClustering {
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        final String pathFile = args[0];
        final int combinersParallelism = Integer.parseInt(args[2]);

        final Time WINDOW_SLIDE = Time.milliseconds(Integer.parseInt(args[5]));
        final Time WINDOW_SIZE = Time.milliseconds(Integer.parseInt(args[6]));

        final int estimatedNumOfKeys = Integer.parseInt(args[7]);
        final int reducersParallelism = Integer.parseInt(args[8]);

        CORR_CLUSTERING clustering_alg;
        if (args[3].equals("VOTE"))
            clustering_alg = CORR_CLUSTERING.VOTE;
        else if (args[3].equals("BEST"))
            clustering_alg = CORR_CLUSTERING.BEST;
        else{
            throw new IllegalArgumentException("Unknown Correlation Clustering Algorithm " + args[3]);
        }

        final OutputTag<PartialClusteringOutput> outputTag = new OutputTag<PartialClusteringOutput>("side-output"){};
        Partitioner partitioner = initializePartitioner(args[4], combinersParallelism, Integer.parseInt(args[5]), Integer.parseInt(args[6]), estimatedNumOfKeys);

        // Configure the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(combinersParallelism);

        // Circular source
        SingleOutputStreamOperator<Tuple2<Integer, Record>> data = env.addSource(new CircularFeed(pathFile), "CircularDataGenerator")
                .slotSharingGroup("source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .slotSharingGroup("source")
                .flatMap(partitioner)
                .slotSharingGroup("source");

        if (args[4].equals("HASHING") || args[4].equals("cAM")){ // Hashing-like techniques
            SingleOutputStreamOperator<List<PartialClusteringOutput>> body = data
                    .keyBy(x -> (x.f0)) // keyBy the key specified by the partitioner
                    .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                    .aggregate(new PartialWindowClustering(clustering_alg)) // correlation clustering in one step
                    .setParallelism(combinersParallelism)
                    .setMaxParallelism(combinersParallelism)
                    .slotSharingGroup("step1");
        }
        else { // key splitting techniques
            SingleOutputStreamOperator<PartialClusteringOutput> body = data
                    .keyBy(x -> (x.f0)) // keyBy the key specified by the partitioner
                    .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                    .aggregate(new PartialWindowClustering(clustering_alg)) // --------- 1st STEP OF THE PROCESSING: Find most similar cluster per block ---------
                    .setParallelism(combinersParallelism)
                    .setMaxParallelism(combinersParallelism)
                    .slotSharingGroup("step1")
                    .process(new ForwardKeys(outputTag)) // forward non-hot keys to the output and hot keys to the reducers
                    .setParallelism(combinersParallelism)
                    .setMaxParallelism(combinersParallelism)
                    .slotSharingGroup("step1");

            DataStream<List<List<String>>> hotResult = body
                    .keyBy(x -> x.blockId)
                    .window(TumblingEventTimeWindows.of(WINDOW_SLIDE))
                    .aggregate(new IncrementalMerge()) // --------- 2nd STEP OF THE PROCESSING: Merge  ---------
                    .setParallelism(reducersParallelism)
                    .setMaxParallelism(reducersParallelism)
                    .slotSharingGroup("step2");
        }

        env.execute("Flink Stream Java API Skeleton");
    }

    public static class ForwardKeys extends ProcessFunction<List<PartialClusteringOutput>, PartialClusteringOutput> {
        OutputTag<PartialClusteringOutput> outputTag;

        public ForwardKeys(OutputTag<PartialClusteringOutput> outputTag){
            this.outputTag = outputTag;
        }

        @Override
        public void processElement(List<PartialClusteringOutput> input, Context ctx, Collector<PartialClusteringOutput> out) {
            for (PartialClusteringOutput block : input) {
                if (block.isHot) {
                    out.collect(block);
                }
                else{
                    ctx.output(outputTag, block);
                }
            }
        }
    }
}
