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

import correlation_clustering.containers.Cluster;
import correlation_clustering.containers.PartialClusteringOutput;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;


/**
 * Merge the clusters of a block
 *
 * Anja Gruenheid, Xin Luna Dong, and Divesh Srivastava.
 * 2014.
 * Incremental record linkage.
 * Proc. VLDB Endow.
 * DOI:https://doi.org/10.14778/2732939.2732943
 *
 * Called after keyBy collects all the results from combiners processing sub-blocks of the same block
 *
 */
public class IncrementalMerge implements AggregateFunction<PartialClusteringOutput, SimilarityGraph, List<List<String>>> {

    class QueueElem {
        public long clusterID;
        public Set<Long> neighbors;

        public QueueElem(long c, Set<Long> n) {
            clusterID = c;
            neighbors = n;
        }
    }

    public IncrementalMerge() {}

    @Override
    public SimilarityGraph createAccumulator() {
        return new SimilarityGraph();
    }

    // Aggregate the newRecord by adding it to the accumulator
    @Override
    public SimilarityGraph add(PartialClusteringOutput input, SimilarityGraph graph) {
        Queue<QueueElem> QC = new LinkedList<>();
        for (Cluster cluster : input.clusters) {
            long newClusterId = cluster.getId();
            Set<Long> neighborClusters = graph.addCluster(newClusterId, cluster.getRecords());
            if (neighborClusters != null){
                QC.add(new QueueElem(newClusterId, neighborClusters));
            }
        }
        QueueElem qe = QC.poll();
        while (qe != null) {
            checkGraph(qe, graph, QC);
            qe = QC.poll();
        }
        return graph;
    }

    public void checkGraph(QueueElem qe, SimilarityGraph graph, Queue<QueueElem> QC) {
        boolean changed;
        // check for merge
        changed = checkMerge(qe, graph, QC);
        // check for split
        if (!changed) {
            changed = checkSplit(qe.clusterID, graph, QC);
            // check for move
            if (!changed) {
                checkMove(qe, graph, QC);
            }
        }
    }

    public boolean checkMerge(QueueElem qe, SimilarityGraph graph, Queue<QueueElem> QC) {
        for (long nId : qe.neighbors) {
            double costBenefit = graph.calcMergeBenefit(qe.clusterID, nId);//check cost benefit from merging
            if (costBenefit > 0) {
                graph.merge(qe.clusterID, nId, qe.neighbors);
                QC.add(new QueueElem(qe.clusterID, qe.neighbors));
                return true;
            }
        }
        return false;
    }

    public boolean checkSplit(long cId, SimilarityGraph graph, Queue<QueueElem> QC) {
        Set<Long> neighborClusters = new HashSet<>();
        long splitId = graph.checkSplit(cId, neighborClusters);
        if (splitId != -1) {
            QC.add(new QueueElem(splitId, neighborClusters));
            return true;
        }
        return false;
    }

    public boolean checkMove(QueueElem qe, SimilarityGraph graph, Queue<QueueElem> QC) {
        boolean changed = false;
        boolean changedC = false;
        for (long nId : qe.neighbors) {
            if (graph.checkMove(qe.clusterID, nId)) {
                changed = true;
                changedC = true;
            }
            if (graph.checkMove(nId, qe.clusterID)) {
                QC.add(new QueueElem(nId, graph.findNeighbors(nId)));
                changed = true;
            }
        }
        if (changedC) {
            QC.add(new QueueElem(qe.clusterID, graph.findNeighbors(qe.clusterID)));
        }
        return changed;
    }

    @Override
    public List<List<String>> getResult(SimilarityGraph graph) {
        return graph.getStrings();
    }

    @Override
    public SimilarityGraph merge(SimilarityGraph a, SimilarityGraph b) {
        // TODO Auto-generated method stub
        return null;
    }
}