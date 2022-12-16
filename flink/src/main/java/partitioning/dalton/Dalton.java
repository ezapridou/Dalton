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

package partitioning.dalton;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import partitioning.Partitioner;
import record.Record;

/**
 * Class implementing the Dalton partitioner for the single-partitioner setup
 */
public class Dalton extends Partitioner implements CheckpointedFunction {
    ContextualBandits cbandit;

    private ListState<ContextualBandits> cbandit_chk;

    public Dalton(int numWorkers, int slide, int size, int numOfKeys){
        super(numWorkers);
        cbandit = new ContextualBandits(numWorkers, slide, size, numOfKeys);
    }

    /**
     * Main function
     * Receives a record
     * @param r the newly arrived record
     * @param out Outputs <Worker, Record>
     */
    @Override
    public void flatMap(Record r, Collector<Tuple2<Integer, Record>> out) throws Exception {
        boolean isHot;
        int worker;

        isHot = cbandit.isHot(r); // checks if it is hot and does maintenance work for hot keys

        cbandit.expireState(r, isHot); // once every slide iterate workers, const. cost per tuple --> O(n)

        worker = cbandit.partition(r, isHot); // partition: iterate workers, const. cost per tuple --> O(n)
        r.setHot(true);

        out.collect(new Tuple2<>(worker, r));

        cbandit.updateState(r, worker); // --> O(n)
        cbandit.updateQtable(r, isHot, worker); // --> O(n)
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        cbandit_chk.clear();
        cbandit_chk.add(cbandit);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        cbandit_chk = functionInitializationContext.getOperatorStateStore().getListState(new ListStateDescriptor<>("cbanditChk", ContextualBandits.class));
        for (ContextualBandits q: cbandit_chk.get()){
            cbandit = q;
        }
    }

    // used for debugging
    public ContextualBandits getCbandit(){
        return cbandit;
    }
}
