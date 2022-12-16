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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.lang.Math;
import java.util.*;

import record.*;
import partitioning.containers.Worker;

/**
 * 2 choices Partitioning
 * <p>
 * Nasir et al.
 * The power of both choices: Practical load balancing for distributed stream processing engines (ICDE'15)
 *
 */

public class TwoChoices extends Partitioner {
    private final double HASH_C = (Math.sqrt(5) - 1) / 2;
    private int nextUpdate;
    private int slide;

    protected List<Worker> workersStats;

    public TwoChoices(int size, int slide, int p) {
        super(p);

        this.slide = slide;
        nextUpdate = 0;
	    workersStats = new ArrayList<>();
        for(int i = 0; i < p; i++){
            Worker w = new Worker(size, slide);
            workersStats.add(w);
        }
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
    public void flatMap(Record record, Collector<Tuple2<Integer, Record>> out) throws Exception {
        int recordId = record.getKeyId();
        int worker1 = hash1(recordId);
        int worker2 = hash2(recordId);

        expireSlide(record.getTs());

        int chosenWorker = (workersStats.get(worker1).getLoad() < workersStats.get(worker2).getLoad()) ? worker1 : worker2;
        updateState(chosenWorker);
        out.collect(new Tuple2<>(chosenWorker, record));
    }

    protected void expireSlide(long now){
        if(now >= nextUpdate){
            for (Worker w : workersStats){
                w.expireOld(now);
                w.addNewSlide(nextUpdate);
            }
            nextUpdate += slide;
        }
    }

    protected void updateState(int worker){
        // increase load of chosen worker
        workersStats.get(worker).updateState();
    }
}

