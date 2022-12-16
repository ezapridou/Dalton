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

package wordCount;

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

import java.util.*;

import partitioning.*;
import record.*;
import sources.*;
import wordCount.containers.*;

import static helperfunctions.PartitionerAssigner.initializePartitioner;

/**
 * <p>
 * Implementation of word count for data streams
 */

public class WordCount {
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        String pathFile = args[0];
        int parallelism = Integer.parseInt(args[2]);

        Time WINDOW_SLIDE = Time.milliseconds(Integer.parseInt(args[5]));
        Time WINDOW_SIZE = Time.milliseconds(Integer.parseInt(args[6]));

        int numOfKeys = Integer.parseInt(args[7]);
        int reducerParallelism = Integer.parseInt(args[8]);

        final OutputTag<WordCountState> outputTag = new OutputTag<WordCountState>("side-output"){};

        // Initialize the partitioner
        Partitioner partitioner1 = initializePartitioner(args[4], parallelism, Integer.parseInt(args[5]), Integer.parseInt(args[6]), numOfKeys);

        // Configure the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(parallelism);

        // Circular source
        SingleOutputStreamOperator<Tuple2<Integer, Record>> data = env.addSource(new CircularFeed(pathFile), "CircularDataGenerator")
                .slotSharingGroup("source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .slotSharingGroup("source")
                .flatMap(partitioner1)
                .slotSharingGroup("source");

        if (args[4].equals("HASHING") || args[4].equals("cAM")) { // Hashing-like techniques
            SingleOutputStreamOperator<WordCountState> body = data
                    .keyBy(x -> (x.f0)) // keyBy the key specified by the partitioner
                    .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                    .aggregate(new MapWordCount()) // word count in one step
                    .setParallelism(parallelism)
                    .setMaxParallelism(parallelism)
                    .process(new SplitPerKeyResult())
                    .setParallelism(parallelism)
                    .setMaxParallelism(parallelism);
        }
        else { // key-splitting techniques
            SingleOutputStreamOperator<WordCountState> body = data
                    .keyBy(x -> (x.f0)) // keyBy the key specified by the partitioner
                    .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                    .aggregate(new MapWordCount()) // --------- 1st STEP OF THE PROCESSING ---------
                    .setParallelism(parallelism)
                    .setMaxParallelism(parallelism)
                    .slotSharingGroup("step1")
                    .process(new ForwardKeys(outputTag))
                    .setParallelism(parallelism)
                    .setMaxParallelism(parallelism)
                    .slotSharingGroup("step1");

            DataStream<WordCountState> hotResult = body
                    .keyBy(x -> x.getKey())
                    .window(TumblingEventTimeWindows.of(WINDOW_SLIDE))
                    .aggregate(new ReduceWordCount()) // --------- 2nd STEP OF THE PROCESSING ---------
                    .setParallelism(reducerParallelism)
                    .setMaxParallelism(reducerParallelism)
                    .slotSharingGroup("step2");

            DataStream<WordCountState> nonHotResult = body.getSideOutput(outputTag);
        }

        env.execute("Flink Stream Java API Skeleton");
    }

    /**
     * Key-forwarding
     * Sends non-hot keys to the output directly and hot keys to the reducers
     */
    public static class ForwardKeys extends ProcessFunction<Map<Integer, WordCountState>, WordCountState> {
        OutputTag<WordCountState> outputTag;

        public ForwardKeys(OutputTag<WordCountState> outputTag){
            this.outputTag = outputTag;
        }

        @Override
        public void processElement(Map<Integer, WordCountState> input, Context ctx, Collector<WordCountState> out){
            for (Map.Entry<Integer, WordCountState> entry : input.entrySet()) {
                if(entry.getValue().isHot()) {
                    out.collect(entry.getValue());
                }
                else{
                    ctx.output(outputTag, entry.getValue());
                }
            }
        }
    }

    public static class SplitPerKeyResult extends ProcessFunction<Map<Integer, WordCountState>, WordCountState> {
        @Override
        public void processElement(Map<Integer, WordCountState> input, Context ctx, Collector<WordCountState> out){
            for (Map.Entry<Integer, WordCountState> entry : input.entrySet()){
                out.collect(entry.getValue());
            }
        }
    }
}
