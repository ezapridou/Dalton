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

package correlation_clustering;

import correlation_clustering.clustering.Best;
import correlation_clustering.clustering.Clustering;
import correlation_clustering.clustering.Vote;
import correlation_clustering.containers.Block;
import correlation_clustering.containers.Cluster;
import correlation_clustering.containers.PartialClusteringOutput;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.IllegalArgumentException;
import java.util.*;

import record.*;

/**
 * Assigns a tuple to the most similar cluster
 */
public class PartialWindowClustering implements AggregateFunction<Tuple2<Integer, Record>, Clustering, List<PartialClusteringOutput>> {

    final private CORR_CLUSTERING algorithm;

    public PartialWindowClustering() {
        this.algorithm = CORR_CLUSTERING.VOTE;
    }

    public PartialWindowClustering(CORR_CLUSTERING algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public Clustering createAccumulator() throws IllegalArgumentException {
        if (algorithm == CORR_CLUSTERING.BEST){
            return new Best();
        }
        else if (algorithm == CORR_CLUSTERING.VOTE){
            return new Vote();
        }
        else{
            throw new IllegalArgumentException("Unknown Correlation Clustering Algorithm" + algorithm);
        }
    }

    // Aggregate the newRecord by adding it to the accumulator
    @Override
    public Clustering add(Tuple2<Integer, Record> input, Clustering accumulator) {
        Map<Integer, Block> stateBlocks = accumulator.getBlocks();
        RecordStr newRecord = (RecordStr)input.f1;

        if (!stateBlocks.containsKey(newRecord.getKeyId())) {
            // create new block
            int id = newRecord.getKeyId();
            Block block = new Block(id, newRecord.isHot());
            Cluster c = new Cluster(newRecord);
            block.addCluster(c);
            stateBlocks.put(id, block);
            return accumulator;
        }

        Cluster mostSimilarCluster = accumulator.findCluster(newRecord);
        if (mostSimilarCluster != null) {
            mostSimilarCluster.add(newRecord);
        } else {
            Cluster c = new Cluster(newRecord);
            stateBlocks.get(newRecord.getKeyId()).addCluster(c);
        }

        return accumulator;
    }

    // return the window result
    @Override
    public List<PartialClusteringOutput> getResult(Clustering accumulator) {
        List<PartialClusteringOutput> output = new ArrayList<>();

        for (Map.Entry<Integer, Block> block : accumulator.getBlocks().entrySet()) {
            List<Cluster> clusters = block.getValue().getClusters();
            output.add(new PartialClusteringOutput(block.getKey(), clusters, block.getValue().isHot()));
        }
        return output;
    }

    @Override
    public Clustering merge(Clustering a, Clustering b) {
        return null;
    }
}