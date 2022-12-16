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

package partitioning;

import record.Record;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import partitioning.dalton.state.State;

import java.util.*;

/**
 * Implementation of DAGreedy algorithm
 *
 * Pacaci et al.
 * Distribution-Aware Stream Partitioning for Distributed Stream Processing Systems (SIGMOD'18 workshop)
 *
 * For the state keeping we use the structures of Dalton
 */
public class DAGreedy extends Partitioner {
    private final State state;
    private Map<Integer, Integer> hotKeys; // key -> expiration timestamp

    public int hash(int key){
        return (key) % parallelism;
    }

    public DAGreedy(int numWorkers, int slide, int size, int numOfKeys){
        super(numWorkers);
        state = new State(size, slide, numWorkers, numOfKeys);
        hotKeys = new HashMap(numOfKeys);
    }

    public boolean isHot(Record r){
        boolean isHot = true;
        int result = state.isHotDAG(r);  // returns 0 if not hot, 1 if hot, expirationTs if it just became hot
        if (result == 0){ // not hot in current window
                if (hotKeys.containsKey(r.getKeyId()) && hotKeys.get(r.getKeyId()) <= r.getTs()){ // hot in previous window (expired)
                    hotKeys.remove(r.getKeyId());
                    isHot = false;
                }
                else if (!hotKeys.containsKey(r.getKeyId())){
                    isHot = false;
                }

        }
        else if (result != 1){ // key just added to this window's hot keys
            if (!hotKeys.containsKey(r.getKeyId())) {
                hotKeys.put(r.getKeyId(), result);
            }
            else{
                hotKeys.put(r.getKeyId(), result);
            }
        }
        // if result == 1 then key was already hot in current window
        return isHot;
    }

    public void expireState(Record r, boolean isHot){
        state.updateExpired(r, isHot);
    }

    public int partition(Record r, boolean isHot){
        return (isHot)?partitionHot(r):hash(r.getKeyId());
    }

    public void updateState(Record r, int worker){
        state.update(r, worker);
    }

    /**
     * Main function
     * Receives a record
     * Returns <Worker, Record>
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Record r, Collector<Tuple2<Integer, Record>> out) throws Exception {
        boolean isHot;
        int worker;

        isHot = isHot(r);
        expireState(r, isHot);
        worker = partition(r, isHot);
        r.setHot(isHot);

        out.collect(new Tuple2<>(worker, r));

        updateState(r, worker);
    }

    public int partitionHot(Record t){
        int worker=0;
        double bestCost = -Double.MAX_VALUE;
        int c1;
        double c2;
        double total;

        BitSet temp;

        for (int i=0; i<parallelism; i++){
            temp = state.keyfragmentation(t.getKeyId());
            if (temp.get(i)){
                c1 = 1;
            }
            else{
                c1=0;
            }
            c2 = state.loadDAG(i);
            total = 0.5 * c1 - 0.5 * c2;
            if (total > bestCost){
                bestCost = total;
                worker = i;
            }
        }
        return worker;
    }
}
