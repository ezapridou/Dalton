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
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class ReduceAggregator extends BaseAggregator<Map<Integer, Map<String, Integer>>> {

    @Override
    public Map<Integer, Map<String, Integer>> init(Object batchId, TridentCollector c){
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<Integer, Map<String, Integer>> stateCounts, TridentTuple tuple, TridentCollector collector){
        if (tuple == null) {
            return;
        }

        Map<Integer, Map<String, Integer>> newCounts = (Map<Integer, Map<String, Integer>>) tuple.get(0);
        for (Map.Entry<Integer, Map<String, Integer>> countsPerKey : newCounts.entrySet()) {
            for (Map.Entry<String, Integer> counts : countsPerKey.getValue().entrySet()) {
                aggregateElem(stateCounts, countsPerKey.getKey(), counts.getKey(), counts.getValue());
            }
        }
    }

    private void aggregateElem(Map<Integer, Map<String, Integer>> stateCounts, int key, String word, int c){
        Map<String, Integer> counts;

        if (stateCounts.containsKey(key)){
            counts = stateCounts.get(key);
        }
        else{
            counts = new HashMap<>();
            stateCounts.put(key, counts);
        }

        if (counts.containsKey(word)){
            counts.put(word, counts.get(word) + c);
        }
        else{
            counts.put(word, c);
        }
    }

    @Override
    public void complete(Map<Integer, Map<String, Integer>> stateCounts, TridentCollector collector){
        for (Map.Entry<Integer, Map<String, Integer>> countsPerKey : stateCounts.entrySet()) {
            for (Map.Entry<String, Integer> counts : countsPerKey.getValue().entrySet()) {
                collector.emit(new Values(countsPerKey.getKey(), counts.getKey(), counts.getValue()));
            }
        }
    }
}
