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

package partitioning.dalton.containers;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Count-Min Sketch data structure.
 *
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 *
 * Code based on:  https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch implements Serializable {

    private static final long PRIME_MODULUS = (1L << 31) - 1;
    private static final long serialVersionUID = -5084982213094657923L;

    private int depth;
    private int width;
    private int[][] table;
    private int[] hashA;
    private int size;
    private double eps;
    private double confidence;

    public CountMinSketch(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
    }

    public CountMinSketch(double epsOfTotalCount, double confidence, int seed) {
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    public void clear(){
        for (int i=0; i<depth; i++){
            for (int j=0; j<width; j++){
                table[i][j] = 0;
            }
        }
    }

    @Override
    public String toString() {
        String str = "start: \n";
        for (int i=0; i<depth; i++){
            for (int j=0; j<width; j++){
                str += table[i][j] +", ";
            }
            str += "\n";
        }
        str+="end \n";
        return str;

    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new int[depth][width];
        this.hashA = new int[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    int hash(int item, int i) {
        int hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return (hash) % width;
    }

    public void add(int item, int count) {
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }
    }

    public int add_and_estimate(int item, int count) {
        int res = Integer.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)] += count);
        }
        return res;
    }

    public int size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    public int estimateCount(int item) {
        int res = Integer.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public int getDepth(){
        return depth;
    }

    public int getWidth(){
        return width;
    }
}