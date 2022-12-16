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

package correlation_clustering.clustering;

import java.util.*;

import record.*;
import correlation_clustering.containers.Block;
import correlation_clustering.containers.Cluster;

/**
 * Implementation of BEST algorithm
 *
 * Micha Elsner and Warren Schudy.
 * 2009.
 * Bounding and comparing methods for correlation clustering beyond ILP.
 * In Proceedings of the Workshop on Integer Linear Programming for Natural Langauge Processing (ILP '09).
 * Association for Computational Linguistics, USA, 19–27.
 */
public class Best extends Clustering{

    public Cluster findCluster(RecordStr newRecord){
        Block block = blocks.get(newRecord.getKeyId());
        Cluster mostSimilarCluster = null;
        double maxNetWeight = 0;

        for (Cluster cluster : block.getClusters()) { // for each cluster
            for (Iterator<RecordStr> it = cluster.getRecords().iterator(); it.hasNext(); ) { // for each record in the cluster
                RecordStr oldRecord = it.next();
                double sim = similarity.similarity(newRecord.getStr(), oldRecord.getStr());
                double netWeight = 2 * sim - 1;
                if (netWeight > maxNetWeight) {
                    maxNetWeight = netWeight;
                    mostSimilarCluster = cluster;
                }
            }

        }
        return mostSimilarCluster;
    }
}