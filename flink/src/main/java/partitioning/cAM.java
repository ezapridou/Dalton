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

/**
 * Implementation of cAM algorithm
 *
 * Nikos R. Katsipoulakis et al.
 * A holistic view of stream partitioning costs. VLB'17
 */
public class cAM extends CardinalityPartitioner {
    public cAM(int size, int slide, int p){
        super(size, slide, p);
    }

    @Override
    public void flatMap(Record record, Collector<Tuple2<Integer, Record>> out) throws Exception{
        int recordId = record.getKeyId();
        int worker1 = hash1(recordId);
        int worker2 = hash2(recordId);
        int chosenWorker;

        if (workersStats.get(worker1).hasSeen(recordId)) {
            // worker1 saw this key before
            chosenWorker = worker1;
        }
        else if(workersStats.get(worker2).hasSeen(recordId)){
            // worker2 saw this key before
            chosenWorker = worker2;
        }
        else{
            // otherwise choose based on tuple count-ups
            chosenWorker = (workersStats.get(worker1).getLoad() < workersStats.get(worker2).getLoad()) ? worker1 : worker2;
        }
        updateState(chosenWorker, recordId);
        out.collect(new Tuple2<>(chosenWorker, record));
    }
}
