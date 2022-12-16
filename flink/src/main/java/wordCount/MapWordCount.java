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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

import record.*;
import wordCount.containers.*;

/**
 * Word count map phase
 * Combiners split the string in words and compute a partial word count per key
 */
public class MapWordCount implements AggregateFunction<Tuple2<Integer, Record>, Map<Integer, WordCountState>, Map<Integer, WordCountState>> {

    public MapWordCount() {
    }

    @Override
    public Map<Integer, WordCountState> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<Integer, WordCountState> add(Tuple2<Integer, Record> input, Map<Integer, WordCountState> accumulator) {
        RecordStr newRecord = (RecordStr)input.f1;
        int id = newRecord.getKeyId();
        String[] words = newRecord.getStr().split(" ");

        for (String word : words){
            if (!accumulator.containsKey(id)){
                WordCountState newState = new WordCountState(id);
                newState.setHot(newRecord.isHot());
                accumulator.put(id, newState);
            }
            else{
                accumulator.get(id).setHot(accumulator.get(id).isHot() || newRecord.isHot());
                accumulator.get(id).countInc(word);
            }
        }
        return accumulator;
    }

    // return the window result
    @Override
    public Map<Integer, WordCountState> getResult(Map<Integer, WordCountState> accumulator) {
        return accumulator;
    }

    @Override
    public Map<Integer, WordCountState> merge(Map<Integer, WordCountState> a, Map<Integer, WordCountState> b) {
        return null;
    }
}