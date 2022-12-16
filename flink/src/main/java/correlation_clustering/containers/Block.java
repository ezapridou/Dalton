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

package correlation_clustering.containers;

import java.util.*;

/**
 * Just an abstraction for a block of records that share the same blocking/partitioning key
 *
 * holds a list with the clusters of the block
 */
public class Block{
    private final int blockId;
    private boolean isHot;
    private List<Cluster> clusters;

    // constructor
    public Block(boolean h){
        blockId = -88;
        clusters = new ArrayList<>();
        isHot = h;
    }

    public Block(int id, boolean h){
        blockId = id;
        clusters = new ArrayList<>();
        isHot = h;
    }

    public List<Cluster> getClusters(){
        return clusters;
    }

    public void addCluster(Cluster c){
        clusters.add(c);
    }

    public int size(){
        return clusters.size();
    }

    public Cluster get(int i){
        return clusters.get(i);
    }

    public String toString(){
        return clusters.toString();
    }

    public boolean isHot(){
        return isHot;
    }

    public void setHot(boolean h){
        isHot = h;
    }

    public void addClusters(List<Cluster> c){
        clusters.addAll(c);
    }
}