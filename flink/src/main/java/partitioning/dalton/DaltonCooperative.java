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
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import partitioning.dalton.containers.syncMessagePartitioners;
import record.Record;
import org.apache.flink.util.OutputTag;
import partitioning.dalton.containers.syncMessage;

/**
 * Class implementing the Dalton partitioner for the multi-agent setup
 */
public class DaltonCooperative extends CoProcessFunction<Record, syncMessage, Tuple2<Integer, Record>>
        implements CheckpointedFunction {

    /**
     * stores a frequent key and its frequency
     * used for the list storing the topKeys (to be forwarded to the QTableReducer)
     */
    public static class Frequency{
        public int key;
        public int freq;

        public Frequency(int k, int f){
            key = k;
            freq = f;
        }

        @Override
        public boolean equals (Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Frequency))
                return false;
            Frequency other = (Frequency)o;
            return this.key == other.key;
        }

        @Override
        public final int hashCode() {
            return key;
        }
    }

    public class Action{
        public double reward;
        public int key;
        public int worker;
        public Action(double r, int k, int w){
            reward = r;
            key = k;
            worker = w;
        }
    }

    private ContextualBandits cbandit;

    private OutputTag<syncMessagePartitioners> outputTag;
    private int syncWindow;
    private int nextSync;
    private int slide;

    private boolean awaits;
    private List<Action> rewardsOnAwait;

    private ListState<ContextualBandits> cbandit_chk;
    private ListState<Integer> sync_chk;

    private List<Frequency> topKeys;

    private int id;

    public DaltonCooperative(int numWorkers, int slide, int size, int numOfKeys, OutputTag<syncMessagePartitioners> outputTag, int id) {
        cbandit = new ContextualBandits(numWorkers, slide, size, numOfKeys);

        this.slide = slide;

        this.outputTag = outputTag;
        syncWindow = slide;
        nextSync = syncWindow;

        awaits = false;
        rewardsOnAwait = new ArrayList<>();

        this.id = id;

        topKeys = new ArrayList<>(numWorkers);
    }

    /**
     * using contextual bandits (or hashing for non-hot keys) decide on the assignment of a record
     * @param r the newly arrived record
     * @param out Outputs <Worker, Record>
     */
    public void recordAssignment(Record r, Collector<Tuple2<Integer, Record>> out) {
        boolean isHot;
        int worker;

        isHot = cbandit.isHot(r, topKeys); // checks if it is hot and does maintenance work for hot keys

        cbandit.expireState(r, isHot); // once every slide iterate workers, const. cost per tuple --> O(n)

        worker = cbandit.partition(r, isHot); // partition: iterate workers, const. cost per tuple --> O(n)
        if (syncWindow <= slide){
            r.setHot(isHot);
        }
        else{
            r.setHot(true); // if syncWindow > slide key-forwarding is disabled for correctness
        }

        out.collect(new Tuple2<>(worker, r));

        cbandit.updateState(r, worker); // --> O(n)
        double reward = cbandit.updateQtable(r, isHot, worker); // --> O(n)
        if (awaits && reward !=-3 ){
            rewardsOnAwait.add(new Action(reward, r.getKeyId(), worker));
        }
    }

    /**
     * @param r an input record
     * @param ctx
     * @param out collector used to output the result (tuple & worker it should be assigned to) to the combiners
     * @throws Exception
     */
    @Override
    public void processElement1(Record r, Context ctx, Collector<Tuple2<Integer, Record>> out) throws Exception {
        recordAssignment(r, out);
        // if the partitioner is about to send a new sync message but is still in await state, propose less frequent sync
        // the QTableReducer cannot keep up with the updates
        if (r.getTs() >= nextSync){
            int proposition = (awaits) ? (syncWindow * 2) : syncWindow;
            ctx.output(outputTag, new syncMessagePartitioners(cbandit.getQtable(), proposition, cbandit.getTotalCountOfRecords(), topKeys));
            nextSync += syncWindow;
            awaits = true;
        }
    }

    /**
     * @param newMsg message received from the QTableReducer
     * @param ctx
     * @param out
     */
    @Override
    public void processElement2(syncMessage newMsg, Context ctx, Collector<Tuple2<Integer, Record>> out){
        if (newMsg.qtable == null){
            return;
        }
        cbandit.setQtable(newMsg.qtable); // use the global qtable
        // update sync interval
        if (syncWindow != newMsg.sync) {
            syncWindow = newMsg.sync;
            cbandit.setHotInterval(syncWindow);
        }
        cbandit.setFrequencyThreshold(newMsg.totalCount); // use the total count for the frequency threshold

        aggrBufferedRewards();
        awaits = false; // exit await state, enter processing state
    }

    /**
     * aggregate rewards buffered during "await" state
     */
    private void aggrBufferedRewards(){
        for (Action action : rewardsOnAwait){
            cbandit.update(action.key, action.worker, action.reward);
        }
        rewardsOnAwait.clear();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        cbandit_chk.clear();
        cbandit_chk.add(cbandit);

        sync_chk.clear();
        sync_chk.add(syncWindow);
        sync_chk.add(nextSync);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        cbandit_chk = functionInitializationContext.getOperatorStateStore().getListState(new ListStateDescriptor<>("cbanditChk"+id, ContextualBandits.class));
        for (ContextualBandits c: cbandit_chk.get()){
            cbandit = c;
        }

        int idx = 0;
        sync_chk = functionInitializationContext.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("syncChk"+id, Integer.class));
        for (int i : sync_chk.get()){
            if (idx == 0){
                syncWindow = i;
            }
            else if (idx == 1){
                nextSync = i;
            }
            idx++;
        }

        rewardsOnAwait = new ArrayList<>();
    }

    // getters/setters used for debugging purposes
    public int getSyncWindow(){
        return syncWindow;
    }

    public void setSyncWindow(int v){
        syncWindow = v;
    }

    public void setNextSync(int v){
        nextSync = v;
    }

    public int getNextSync(){
        return nextSync;
    }

    public boolean getAwaits(){
        return awaits;
    }

    public ContextualBandits getCbandit(){
        return cbandit;
    }

    public List<DaltonCooperative.Frequency> getTopKeys(){
        return topKeys;
    }
}
