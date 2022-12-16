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

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import partitioning.prompt.Prompt;

import java.util.ArrayList;
import java.util.List;

public class ShufflePartitioner extends BaseAggregator<Integer> {

    private final int parallelism;
    private int idx;

    List<Prompt.Tuple> batch = new ArrayList<>();

    public ShufflePartitioner(int parallelism){
        this.parallelism = parallelism;
        idx = 0;
    }

    @Override
    public Integer init (Object batchId, TridentCollector collector){
        return 0;
    }

    @Override
    public void aggregate(Integer state, TridentTuple tuple, TridentCollector collector){
        int key = tuple.getInteger(0);
        batch.add(new Prompt.Tuple(key, tuple.getString(1)));
    }

    @Override
    public void complete(Integer state, TridentCollector tridentCollector) {
        for (Prompt.Tuple tuple : batch){
            tridentCollector.emit(new Values(tuple.key, tuple.str, idx));
            idx++;
            if (idx == parallelism){
                idx = 0;
            }
        }
        batch.clear();
    }

}
