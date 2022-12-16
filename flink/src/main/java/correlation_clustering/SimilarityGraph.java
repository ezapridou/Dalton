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

import correlation_clustering.stringsimilarity.Similarity;
import record.*;

import java.util.*;


/**
 * Stores the similarity graph
 * For every record it stores its neighbors (only the ones with higher id than the record's id) and its similarity with them
 *
 */
public class SimilarityGraph{

    /**
     * struct containing a record, its cluster id and its neighbors
     */
    class RecordSim
    {
        private RecordStr record;
        private long clusterId;
        private Map<Long, Double> similarities; //similarity per neighbor

        public RecordSim(RecordStr r, long cid){
            record = r;
            clusterId = cid;
            similarities = new HashMap<>();
        }

        public String getRecordStr(){
            return record.getStr();
        }

        public long getClusterId() {
            return clusterId;
        }

        public RecordStr getRecord() {
            return record;
        }

        public Map<Long, Double> getSimilarities(){
            return similarities;
        }

        public void changeCluster(long cId){
            clusterId = cId;
        }

        public void addNeighbor(long nid, double sim){
            similarities.put(nid, sim);
        }

        public double getSimilarity(long nid){
            return similarities.getOrDefault(nid, 0.0);
        }
    }

    private Map<Long, RecordSim> graph; // neighbors per record id
    private Map<Long, Set<Long>> clusters;
    Similarity similarity;

    // constructor
    public SimilarityGraph(){
        graph = new HashMap<>();
        clusters = new HashMap<>();
        similarity = new Similarity();
    }

    public void addNeighbor(long rid, long nid, double similarity){
        if (rid < nid){
            if (graph.containsKey(rid)){
                graph.get(rid).addNeighbor(nid, similarity);
            }
        }
        else{
            if (graph.containsKey(nid)){
                graph.get(nid).addNeighbor(rid, similarity);
            }
        }
    }

    public void addNeighbor(long rid, String r, long nid, String n){
        addNeighbor(rid, nid, similarity.similarity(r, n));
    }

    public double getSimilarity(long rid, long nid){
        if (rid < nid){
            return graph.get(rid).getSimilarity(nid);
        }
        else{
            return graph.get(nid).getSimilarity(rid);
        }
    }

    public String getRecordStr(long rid){
        if(graph.containsKey(rid)){
            graph.get(rid).getRecordStr();
        }
        return null;
    }

    private Set<Long> addRecord(RecordStr r, long clusterId){
        long rid = r.getTs();
        RecordSim recSim = new RecordSim(r, clusterId);
        Set<Long> neighborClusters = new HashSet<>();
        double simVal;
        for(Map.Entry<Long, RecordSim> entry : graph.entrySet()) {
            simVal = similarity.similarity(r.getStr(), entry.getValue().getRecordStr());
            if (simVal > 0){
                neighborClusters.add(entry.getValue().getClusterId());
                if (entry.getKey() < rid){
                    entry.getValue().addNeighbor(rid, simVal);
                }
                else{
                    recSim.addNeighbor(entry.getKey(), simVal);
                }
            }
        }
        graph.put(rid, recSim);
        return neighborClusters;
    }

    public Set<Long> addCluster(long clusterId, Set<RecordStr> tuples){
        Set<Long> newCluster = new HashSet<>();
        Set<Long> neighborClusters = new HashSet<>();
        for (RecordStr tuple : tuples) {
            Set<Long> nc = addRecord(tuple, clusterId);
            newCluster.add(tuple.getTs());
            neighborClusters.addAll(nc);
        }
        if (!tuples.isEmpty()){
            neighborClusters.remove(clusterId);
            clusters.put(clusterId, newCluster);
            return neighborClusters;
        }
        return null;
    }

    public Map<Long, Set<RecordStr>> getClusters(){
        Map<Long, Set<RecordStr>> res = new HashMap<>();
        for (long clusterId : clusters.keySet()){
            Set<RecordStr> cluster = new HashSet<>();
            for (long recordId : clusters.get(clusterId)){
                cluster.add(graph.get(recordId).getRecord());
            }
            res.put(clusterId, cluster);
        }

        return res;
    }

    public int numOfClusters(){
        return clusters.size();
    }

    public Set<Long> getCluster(long id){
        return clusters.get(id);
    }

    public String toString(){
        return clusters.toString();
    }

    public double calcMergeBenefit(long c1Id, long c2Id){
        Set<Long> c1 = clusters.get(c1Id);
        Set<Long> c2 = clusters.get(c2Id);

        if (c2 == null || c1 == null){
            return 0;
        }
        double costBenefit = 0;
        //for every record in c1 if it exists in c2 add cost
        for(long r1Id : c1){
            for (long r2Id : c2){
                costBenefit += 2 * getSimilarity(r1Id, r2Id) - 1;
            }
        }
        return costBenefit;
    }

    public void merge(long c1Id, long c2Id, Set<Long> neighborClusters){
        Set<Long> c1=clusters.get(c1Id);
        Set<Long> c2=clusters.get(c2Id);
        c1.addAll(c2);

        for (long rid : c2){
            graph.get(rid).changeCluster(c1Id);
            // find neighbors
            for (long tid : graph.get(rid).getSimilarities().keySet()){
                neighborClusters.add(graph.get(tid).getClusterId());
            }
        }
        clusters.remove(c2Id);
    }

    public long checkSplit(long cId, Set<Long> neighborClusters){
        Set<Long> c = clusters.get(cId);
        if (c == null){
            return -1;
        }
        for (long toSplit : c){
            //check if it is beneficial to split this record out of the cluster
            double splitBenefit = 0;
            for (long nId : c){
                if (nId != toSplit) {
                    splitBenefit += 1 - 2 * getSimilarity(toSplit, nId);
                }
            }
            if (splitBenefit > 0){
                // find neighbors
                for (long tid : graph.get(toSplit).getSimilarities().keySet()){
                    neighborClusters.add(graph.get(tid).getClusterId());
                }

                //check whether toSplit is a valid id for new cluster
                long newClusterId = toSplit;
                while (graph.containsKey(newClusterId)){
                    newClusterId+=1000;
                }
                Set<Long> newCluster = new HashSet<>();
                newCluster.add(toSplit);
                clusters.put(newClusterId, newCluster);
                c.remove(toSplit);
                graph.get(toSplit).changeCluster(newClusterId);

                //for every node of the old cluster check whether it should be moved to the new cluster
                for (Iterator<Long> it = c.iterator(); it.hasNext(); ){
                    long nId = it.next();
                    double moveBenefit = 0;
                    for (long tId : c){
                        if (tId != nId){
                            moveBenefit += 1 - 2 * getSimilarity(nId, tId);
                        }
                    }
                    for (long tId : newCluster){
                        moveBenefit += getSimilarity(nId, tId) - 1;
                    }
                    if (moveBenefit > 0){
                        newCluster.add(nId);
                        it.remove();
                        graph.get(nId).changeCluster(newClusterId);
                        // find neighbors
                        for (long tid : graph.get(nId).getSimilarities().keySet()){
                            neighborClusters.add(graph.get(tid).getClusterId());
                        }
                    }
                }
                return newClusterId;
            }
        }
        return -1;
    }

    public boolean checkMove(long oldClusterId, long newClusterId){
        boolean moved = false;

        Set<Long> oldCluster = clusters.get(oldClusterId);
        Set<Long> newCluster = clusters.get(newClusterId);
        if (newCluster == null || oldCluster == null){
            return false;
        }
        for (Iterator<Long> it = oldCluster.iterator(); it.hasNext(); ){
            long oId = it.next();
            double moveBenefit = 0;
            for (long tId : oldCluster){
                if (tId != oId){
                    moveBenefit += 1 - 2 * getSimilarity(oId, tId);
                }
            }
            for (long nId : newCluster){
                moveBenefit += getSimilarity(oId, nId) - 1;
            }
            if (moveBenefit > 0){
                moved = true;
                newCluster.add(oId);
                it.remove();
                graph.get(oId).changeCluster(newClusterId);
            }
        }
        return moved;
    }

    public Set<Long> findNeighbors(long clusterId){
        Set<Long> cluster = clusters.get(clusterId);
        if (cluster == null){
            return null;
        }
        Set<Long> neighborClusters = new HashSet<>();
        for (long rid : cluster){
            // find neighbors
            for (long tid : graph.get(rid).getSimilarities().keySet()){
                neighborClusters.add(graph.get(tid).getClusterId());
            }
        }
        return neighborClusters;
    }

    public List<List<String>> getStrings(){
        List<List<String>> list = new ArrayList<>();
        for (long clusterId : clusters.keySet()){
            List<String> res = new ArrayList<>();
            for (long tid : clusters.get(clusterId)){
                res.add(graph.get(tid).getRecordStr());
            }
            list.add(res);
        }

        return list;
    }
}