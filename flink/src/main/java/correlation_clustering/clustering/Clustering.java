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
import correlation_clustering.stringsimilarity.Similarity;

/**
 * Abstract class for clustering algorithms
 */
public abstract class Clustering{
    protected Similarity similarity;
    protected Map<Integer, Block> blocks;
    protected boolean sampling = false;

    public Clustering(boolean sampling){
        similarity = new Similarity();
        blocks = new HashMap<>();
        this.sampling = sampling;
    }

    public Clustering(){
        similarity = new Similarity();
        blocks = new HashMap<>();
    }

    public Map<Integer, Block> getBlocks() {
        return blocks;
    }

    public abstract Cluster findCluster(RecordStr t);
}