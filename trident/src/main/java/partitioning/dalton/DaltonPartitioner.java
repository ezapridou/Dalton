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

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import partitioning.prompt.Prompt;

import java.util.ArrayList;
import java.util.List;

public class DaltonPartitioner extends BaseAggregator<Integer> {

    private ContextualBandits cbandit;
    private int numOfBatch;
    private int batchSize;
    List<Prompt.Tuple> batch = new ArrayList<>();
    private int numTuplesInBatch;

    public DaltonPartitioner(int parallelism, int slide, int size, int numOfKeys, int batchSize){
        cbandit = new ContextualBandits(parallelism, slide, size, numOfKeys);
        numOfBatch = 0;
        this.batchSize = batchSize;
        numTuplesInBatch = 0;
    }

    @Override
    public Integer init (Object batchId, TridentCollector collector){
        return 0;
    }

    @Override
    public void aggregate(Integer state, TridentTuple tuple, TridentCollector collector){
        int keyId = tuple.getInteger(0);
        batch.add(new Prompt.Tuple(keyId, tuple.getString(1)));
    }

    @Override
    public void complete(Integer state, TridentCollector tridentCollector) {
        for (Prompt.Tuple tuple : batch){
            chooseWorker(tuple, tridentCollector);
        }
        batch.clear();

        numOfBatch++;
        numTuplesInBatch = 0;
    }

    private void chooseWorker(Prompt.Tuple tuple, TridentCollector collector){
        int keyId = tuple.key;
        int ts = numOfBatch * batchSize + numTuplesInBatch;
        numTuplesInBatch++;

        boolean isHot = cbandit.isHot(keyId, ts);

        cbandit.expireState(ts, isHot); // here

        int worker = cbandit.partition(keyId, isHot);

        collector.emit(new Values(keyId, tuple.str, worker));

        cbandit.updateState(keyId, worker);
        cbandit.updateQtable(keyId, isHot, worker);
    }
}
