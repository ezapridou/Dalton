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

import java.lang.Math;
import java.util.*;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import partitioning.prompt.Prompt;

/**
 * 2 choices Partitioning
 * <p>
 * Nasir et al.
 * The power of both choices: Practical load balancing for distributed stream processing engines (ICDE'15)
 *
 */

public class TwoChoices extends BaseAggregator<Integer> {
    private final double HASH_C = (Math.sqrt(5) - 1) / 2;
    private int parallelism;
    private List<Integer> workersLoad;

    List<Prompt.Tuple> batch = new ArrayList<>();

    public TwoChoices(int p) {
        parallelism = p;
    }

    @Override
    public Integer init (Object batchId, TridentCollector collector){
        workersLoad = new ArrayList<>();
        for(int i = 0; i < parallelism; i++){
            workersLoad.add(0);
        }
        return 0;
    }

    protected int hash1(int n) {
        return n % parallelism;
    }

    // https://www.geeksforgeeks.org/what-are-hash-functions-and-how-to-choose-a-good-hash-function/
    protected int hash2(int n) {
        double a = (n + 1) * HASH_C;
        return (int)Math.floor(parallelism * (a - (int) a));
    }

    @Override
    public void aggregate(Integer state, TridentTuple tuple, TridentCollector collector) {
        int key = tuple.getInteger(0);
        batch.add(new Prompt.Tuple(key, tuple.getString(1)));
    }

    protected void updateState(int worker, List<Integer> workersStats){
        // increase load of chosen worker
        workersStats.set(worker, workersStats.get(worker));
    }

    @Override
    public void complete(Integer state, TridentCollector tridentCollector){
        for (Prompt.Tuple tuple : batch){
            int recordId = tuple.key;
            int worker1 = hash1(recordId);
            int worker2 = hash2(recordId);

            // choose the worker with lowest load
            int chosenWorker = (workersLoad.get(worker1) < workersLoad.get(worker2)) ? worker1 : worker2;
            updateState(chosenWorker, workersLoad);
            tridentCollector.emit(new Values(tuple.key, tuple.str, chosenWorker));
        }
        batch.clear();
    }
}

