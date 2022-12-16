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

package partitioning.prompt;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Prompt: Dynamic Data-Partitioning for Distributed Micro-batch Stream Processing Systems
 * Abdelhamid et al.
 *
 * Note that we could not find existing code for this work so we implemented it from scratch
 */
public class Prompt extends BaseAggregator<Integer> {

    public class TuplesPerWorker{
        int worker;
        int tupleCount;

        public TuplesPerWorker(int w, int t){
            worker = w;
            tupleCount = t;
        }
    }

    public static class Tuple{
        public int key;
        public String str;

        public Tuple(int k, String s){
            key = k;
            str = s;
        }
    }

    private int numPartitions;
    int batchSize;
    private Stats stats; // Map (key -> count)
    private Set<Integer> setKeys = new HashSet<Integer>(); // all keys that exist
    Map<Integer, List<TuplesPerWorker>> keyMap = new HashMap<Integer, List<TuplesPerWorker>>(); // key -> (worker -> numOfTuples)
    List<Tuple> batch = new ArrayList<>();

    public Prompt(int numPartitions, int batchSize) {
        this.batchSize = batchSize;
        this.numPartitions = numPartitions;
        stats = new Stats();
    }

    @Override
    public Integer init (Object batchId, TridentCollector collector){
        return 0;
    }

    @Override
    public void aggregate(Integer state, TridentTuple tuple, TridentCollector collector){
        int key = tuple.getInteger(0);
        addKey(key);
        setKeys.add(key);
        batch.add(new Tuple(key, tuple.getString(1)));
    }

    @Override
    public void complete(Integer state, TridentCollector tridentCollector) {
        calcPartitions();
        for (Tuple tuple : batch){
            tridentCollector.emit(new Values(tuple.key, tuple.str, getPartition(tuple.key)));
        }
        clear();
    }

    public int getPartition(int currentKey){
        int j = 0;
        while (j<keyMap.get(currentKey).size() && keyMap.get(currentKey).get(j).tupleCount == 0){
            j = j +1;
        }
        int worker = keyMap.get(currentKey).get(j).worker;
        keyMap.get(currentKey).get(j).tupleCount -= 1;
        return worker;
    }

    public void clear(){
        keyMap.clear();
        setKeys.clear();
        stats.clear();
        batch.clear();
    }

    public void addKey(int key){
        stats.incCount(key);
    }

    public void calcPartitions(){
        int psize = batchSize/numPartitions; // tuples per partition
        int pk = stats.size()/numPartitions;
        int scut = psize/pk;

        int bi = 0;
        boolean stillHot = true;

        Map<Integer, Integer> partitionsCount = new HashMap<Integer, Integer>(); // partition -> tuple count
        int k=0;
        Map<Integer, Integer> residuals = new HashMap<Integer, Integer>(); // key -> tuples left

        // HOT KEYS - PHASE 1
        int totalVisited = 0;
        while (totalVisited <= setKeys.size() && stillHot){
            while (!setKeys.contains(k) && totalVisited<=setKeys.size()){
                k = k + 1;
            }
            if (stats.getKey(k) > scut){
                List n = new ArrayList<TuplesPerWorker>();
                n.add(new TuplesPerWorker(bi, scut));
                keyMap.put(k, n);
                totalVisited++;
                residuals.put(k, stats.getKey(k) - scut);
                partitionsCount.put(bi, scut);
                bi = (bi+1) % numPartitions;
                k++;
            }
            else {
                stillHot = false;
            }
        }
        // NON HOT - PHASE 2
        int p = 0;
        boolean add = true;
        while (totalVisited<=setKeys.size()){
            while (!setKeys.contains(k) && totalVisited<=setKeys.size()){
                k++;
            }
            List n = new ArrayList<TuplesPerWorker>();
            n.add(new TuplesPerWorker(p, stats.getKey(k)));
            keyMap.put(k, n);
            partitionsCount.put(p, partitionsCount.getOrDefault(p, 0) + stats.getKey(k));
            totalVisited++;
            if (add) {
                p ++;
                if (p == numPartitions){
                    p--;
                    add = false;
                }
            }
            else{
                p--;
                if (p == -1){
                    p = 0;
                    add = true;
                }
            }
            k++;
        }

        // HOT RESIDUALS - PHASE 3
         for (Map.Entry<Integer, Integer> entry : residuals.entrySet()) {
             int key = entry.getKey();
             int numTuples = entry.getValue();
             int tuplesInPartitionOfKey = partitionsCount.get(keyMap.get(key).get(0).worker);
            if (psize > tuplesInPartitionOfKey + numTuples){ // if it fits in the same partition as I put it in phase 1
                keyMap.get(key).get(0).tupleCount = stats.getKey(key);
                partitionsCount.put(keyMap.get(key).get(0).worker, partitionsCount.get(keyMap.get(key).get(0).worker) + numTuples);
            }
            else{
                // find the smallest one that fits it
                int bestFit = Integer.MAX_VALUE;
                int bestFitW = -1;
                int mostEmpty = Integer.MIN_VALUE;
                int mostEmptyW = -1;
                for (int ps = 0; ps < numPartitions; ps++){
                    if ((psize - partitionsCount.getOrDefault(ps, 0)) - numTuples < bestFit && (psize - partitionsCount.getOrDefault(ps, 0)) >= numTuples){
                        bestFit = (psize - partitionsCount.getOrDefault(ps, 0)) - numTuples;
                        bestFitW = ps;
                    }
                    if (psize - partitionsCount.getOrDefault(ps, 0) > mostEmpty){
                        mostEmpty = psize - partitionsCount.getOrDefault(ps, 0);
                        mostEmptyW = ps;
                    }
                }
                if (bestFitW != -1){
                    keyMap.get(key).add(new TuplesPerWorker(bestFitW, numTuples));
                    partitionsCount.put(bestFitW, partitionsCount.getOrDefault(bestFitW, 0)+numTuples);
                }
                else{
                    keyMap.get(key).add(new TuplesPerWorker(mostEmptyW, numTuples));
                    partitionsCount.put(mostEmptyW, partitionsCount.getOrDefault(mostEmptyW, 0)+numTuples);
                }
            }
        }
    }

}
