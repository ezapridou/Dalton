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

import java.util.*;

import wordCount.containers.WordCountState;

/**
 * Reduce method for the word count
 * Reducer compute the final word counts per key
 */
public class ReduceWordCount implements AggregateFunction<WordCountState, WordCountState, WordCountState> {

    public ReduceWordCount() {}

    @Override
    public WordCountState createAccumulator() {
        return null;
    }

    @Override
    public WordCountState add(WordCountState input, WordCountState accumulator) {
        Map<String, Integer> partialCounts = input.getCounts();

        if (accumulator != null){
            for (Map.Entry<String, Integer> elem : partialCounts.entrySet()){
                accumulator.countInc(elem.getKey(), elem.getValue());
            }
        }
        else{
            accumulator = input;
        }
        return accumulator;
    }

    // return the window result
    @Override
    public WordCountState getResult(WordCountState accumulator) {
        return accumulator;
    }

    @Override
    public WordCountState merge(WordCountState a, WordCountState b) {
        return null;
    }
}