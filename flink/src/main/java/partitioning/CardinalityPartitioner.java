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

import partitioning.containers.CardinalityWorker;

import java.util.*;

/**
 * abstract class for the implementation of cardinality partitioners
 * We implement load statistics similarly to the implementation for Dalton
 *
 * Nikos R. Katsipoulakis et al.
 * A holistic view of stream partitioning costs. VLB'17
 */
public abstract class CardinalityPartitioner extends Partitioner {
    private final double HASH_C = (Math.sqrt(5) - 1) / 2;
    private int nextUpdate;
    private final int slide;

    protected List<CardinalityWorker> workersStats;


    public CardinalityPartitioner(int size, int slide, int p){
        super(p);
        this.slide = slide;
        nextUpdate = 0;

        workersStats = new ArrayList<>();
        for(int i = 0; i < p; i++){
            CardinalityWorker w = new CardinalityWorker(size, slide);
            workersStats.add(w);
        }
    }

    protected int hash1(int n) {
        return n % parallelism;
    }

    // https://www.geeksforgeeks.org/what-are-hash-functions-and-how-to-choose-a-good-hash-function/
    protected int hash2(int n) {
        double a = (n + 1) * HASH_C;
        return (int)Math.floor(parallelism * (a - (int) a));
    }

    protected void expireSlide(long now){
        if(now >= nextUpdate){
            for (CardinalityWorker w : workersStats){
                w.expireOld(now);
                w.addNewSlide(nextUpdate);
            }
            nextUpdate += slide;
        }
    }

    protected void updateState(int worker, int key){
        workersStats.get(worker).updateState(key);
    }
}
