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

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MapAggregator extends BaseAggregator<Map<Integer, Map<String, Integer>>> {

    public MapAggregator(){

    }

    @Override
    public Map<Integer, Map<String, Integer>> init(Object batchId, TridentCollector collector){
        return new HashMap<Integer, Map<String, Integer>>();
    }

    @Override
    public void aggregate(Map<Integer, Map<String, Integer>> stateCounts, TridentTuple tuple, TridentCollector collector){
        String[] words = tuple.getString(1).split(" ");
        Map<String, Integer> counts;
        if (stateCounts.containsKey(tuple.getInteger(0))){
            counts = stateCounts.get(tuple.getInteger(0));
        }
        else{
            counts = new HashMap<>();
            stateCounts.put(tuple.getInteger(0), counts);
        }

        for (String word : words) {
            if (counts.containsKey(word)) {
                counts.put(word, counts.get(word)+1);
            } else {
                counts.put(word, 1);
            }
        }
    }

    @Override
    public void complete(Map<Integer, Map<String, Integer>> stateCounts, TridentCollector collector){
        collector.emit(new Values(stateCounts));
    }
}
