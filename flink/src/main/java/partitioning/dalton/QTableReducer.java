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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import partitioning.dalton.containers.Qtable;
import partitioning.dalton.containers.QtableEntry;
import partitioning.dalton.containers.syncMessage;
import partitioning.dalton.containers.syncMessagePartitioners;

import java.util.HashMap;
import java.util.Map;

/**
 * Class implementing the QTableRedcuer
 * It collects the synchronization messages from the partitioners, aggregates them and
 * broadcasts to the partitioners the global Qtable among with the global count and the update syncrhonization interval
 */
public class QTableReducer implements FlatMapFunction<syncMessagePartitioners, syncMessage> {
    public Qtable globalQTable;
    public int msgsReceived; // number of messages received from the partitioners
    private final int numOfPartitioners;
    private final int numOfWorkers;
    public int syncInterval;
    public int totalNumOfRecords; // seen in the past sync window
    private Map<Integer, Integer> perKeyCounts; // global count for a key
    private int INIT_VAL = -2; // initial value for qvalues
    private int eventTime;
    public long idleTime;
    public long processingTime;
    private boolean increaseSyncInterval; // whether a partitioner asked for less frequent synchronization

    public QTableReducer(int numOfPartitioners, int numOfWorkers){
        msgsReceived = 0;
        this.numOfPartitioners = numOfPartitioners;
        totalNumOfRecords = 0;
        perKeyCounts = new HashMap<>();
        this.numOfWorkers = numOfWorkers;
        eventTime = 0;
        idleTime = 0;
        processingTime = 0;
        increaseSyncInterval = false;
    }

    /**
     *
     * @param input a sync message from a partitioner (contains localQTable, topKeys, recordCount, syncInterval proposal)
     * @param out message containing the globalQTable, globalRecordCount, new syncInterval (broadcasted to the partitioners)
     * @throws Exception
     */
    @Override
    public void flatMap(syncMessagePartitioners input, Collector<syncMessage> out) throws Exception {
        if (globalQTable == null){
            globalQTable = input.qtable;
            msgsReceived++;
            syncInterval = input.sync;
            totalNumOfRecords += input.totalCount;
            long now = System.currentTimeMillis();
            idleTime = now - idleTime;
        }
        else{
            if (msgsReceived == 0){
                long now = System.currentTimeMillis();
                idleTime = now - idleTime;
                processingTime = now;
            }
            aggregateMsg(input);
            msgsReceived++;
            totalNumOfRecords += input.totalCount;
            if (msgsReceived == numOfPartitioners){
                divideByCount();
                // find all hot keys
                extractHot();
                processingTime = System.currentTimeMillis() - processingTime;
                // if none of the partitioners requests less frequent sync and idle time > 2 * processing time
                // then suggest more frequent sync
                if (!increaseSyncInterval && idleTime > 2 * processingTime){
                    syncInterval /= 2;
                }
                out.collect(new syncMessage(globalQTable, syncInterval, totalNumOfRecords));
                clear();
            }
        }
    }

    public void clear(){
        msgsReceived = 0;
        globalQTable.clear();
        totalNumOfRecords = 0;
        perKeyCounts.clear();
        eventTime += syncInterval;
        idleTime = System.currentTimeMillis();
        increaseSyncInterval = false;
    }

    /**
     * Find keys that are hot for the global distribution but where not hot separately for any local distribution
     * (they did not have an entry in any local qtable)
     */
    public void extractHot(){
        for (Map.Entry<Integer, Integer> entry : perKeyCounts.entrySet()){
            if (!globalQTable.containsKey(entry.getKey()) && entry.getValue() > totalNumOfRecords/numOfWorkers){
                initializeKey(entry.getKey(), eventTime + syncInterval);
            }
        }
    }

    private void initializeKey(int key, int expTs){
        double[] actions = new double[numOfWorkers];
        for(int j = 0; j < numOfWorkers; j++){
            actions[j] = INIT_VAL;
        }
        globalQTable.put(key, actions, expTs);
    }

    /**
     * Divide qvalues of a key by the globalCount of the key
     * (weighted average)
     */
    public void divideByCount(){ // divide by per-key count
        for (Map.Entry<Integer, QtableEntry> entry : globalQTable.getTable().entrySet()){
            for (int i=0; i<entry.getValue().qvalues.length; i++){
                entry.getValue().qvalues[i]/= perKeyCounts.get(entry.getKey());
            }
        }
    }

    public void aggregateMsg(syncMessagePartitioners input){
        // check whether partitioners suggested larger sync interval
        if (syncInterval < input.sync){
            increaseSyncInterval = true;
            syncInterval = input.sync;
        }
        // weighted average (aggregate local qtable) (division by total count happens at the end --> divideByCount())
        for (DaltonCooperative.Frequency entry : input.topKeys){
            int count = entry.freq;
            int prevCount = perKeyCounts.containsKey(entry.key)? perKeyCounts.get(entry.key) : 0;
            perKeyCounts.put(entry.key, prevCount+count);

            if (input.qtable.containsKey(entry.key)){
                double[] qEntry = input.qtable.get(entry.key);
                try {
                    if (input.qtable.getExpirationTs(entry.key) > globalQTable.getTable().get(entry.key).expirationTs){
                        globalQTable.getTable().get(entry.key).expirationTs = input.qtable.getExpirationTs(entry.key);
                    }

                    for (int i = 0; i < qEntry.length; i++) {
                        globalQTable.get(entry.key)[i] += count * qEntry[i];
                    }
                } catch (NullPointerException ex){
                    globalQTable.put(entry.key, qEntry, input.qtable.getExpirationTs(entry.key));
                }
            }
        }
    }
}
