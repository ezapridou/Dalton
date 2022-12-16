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
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import partitioning.dalton.*;
import partitioning.dalton.containers.*;
import record.*;
import sources.*;
import wordCount.containers.*;

/**
 * <p>
 * Implementation of word count for data streams
 * Pipeline used for the multi-agent setup and the Dalton partitioner
 */

public class WordCountMultiAgent {
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        // parameters
        String[] pathFiles = new String[2];
        pathFiles[0] = args[0];
        pathFiles[1] = args[9];
        int combinersParallelism = Integer.parseInt(args[2]);
        int slide = Integer.parseInt(args[5]);
        int size = Integer.parseInt(args[6]);
        int numOfKeys = Integer.parseInt(args[7]);
        int reducerParallelism = Integer.parseInt(args[8]);

        Time WINDOW_SLIDE = Time.milliseconds(slide);
        Time WINDOW_SIZE = Time.milliseconds(size);

        // output tags
        final OutputTag<WordCountState> outputTag = new OutputTag<WordCountState>("side-output"){};
        final OutputTag<syncMessagePartitioners> outputTag1 = new OutputTag<syncMessagePartitioners>("upd1"){};
        final OutputTag<syncMessagePartitioners> outputTag2 = new OutputTag<syncMessagePartitioners>("upd2"){};

        // Initialize partitioners
        DaltonCooperative partitioner1 = new DaltonCooperative(combinersParallelism, slide, size, numOfKeys, outputTag1, 1);
        DaltonCooperative partitioner2 = new DaltonCooperative(combinersParallelism, slide, size, numOfKeys, outputTag2, 2);

        // Configure the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(combinersParallelism);

        // Initialize iteration
        DataStream<syncMessage> init = env.fromElements(new syncMessage(null, 0, 0));
        IterativeStream<syncMessage> iteration = init.iterate();

        // Circular sources
        SingleOutputStreamOperator<Tuple2<Integer, Record>> data1 = env.addSource(new CircularFeed2files(pathFiles, 1, false, 2), "CircularDataGenerator")
                .slotSharingGroup("source1")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .slotSharingGroup("source1")
                .connect(iteration)
                .process(partitioner1)
                .slotSharingGroup("source1");
        SingleOutputStreamOperator<Tuple2<Integer, Record>> data2 = env.addSource(new CircularFeed2files(pathFiles, 2, false, 2), "CircularDataGenerator")
                .slotSharingGroup("source2")
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .slotSharingGroup("source2")
                .connect(iteration)
                .process(partitioner2)
                .slotSharingGroup("source2");

        // Send Q table updates to master
        DataStream<syncMessage> masterUpdates = data1.getSideOutput(outputTag1)
                .union(data2.getSideOutput(outputTag2))
                .flatMap(new QTableReducer(2, combinersParallelism));
        iteration.closeWith(masterUpdates);

        // Execute word count task
        SingleOutputStreamOperator<WordCountState> body = data1
                .union(data2)
                //.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(x -> (x.f0)) // keyBy the key specified by the partitioner
                .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                .aggregate(new MapWordCount()) // --------- 1st STEP OF THE PROCESSING ---------
                .setParallelism(combinersParallelism)
                .setMaxParallelism(combinersParallelism)
                .slotSharingGroup("step1")
                .process(new WordCount.ForwardKeys(outputTag))
                .setParallelism(combinersParallelism)
                .setMaxParallelism(combinersParallelism)
                .slotSharingGroup("step1");

        DataStream<WordCountState> hotResult = body
                .keyBy(x -> x.getKey())
                .window(TumblingEventTimeWindows.of(WINDOW_SLIDE))
                .aggregate(new ReduceWordCount()) // --------- 2nd STEP OF THE PROCESSING ---------
                .setParallelism(reducerParallelism)
                .setMaxParallelism(reducerParallelism)
                .slotSharingGroup("step2");

        DataStream<WordCountState> nonHotResult = body.getSideOutput(outputTag);

        env.execute("Flink Stream Java API Skeleton");
    }

}
