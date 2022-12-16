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

package partitioning.containers;

import java.io.Serializable;
import java.util.*;

/**
 * Class responsible for the maintenance of load statistic for the cardinality partitioners
 */
public class CardinalityWorker implements Serializable {
    private Queue<PartialLoadCard> memPool;
    private Queue<PartialLoadCard> activeSlides;
    private PartialLoadCard lastSlide;
    private int size;
    private int aggrLoad;
    private BitSet aggrCard;

    public CardinalityWorker(int size, int slide){
        this.activeSlides = new LinkedList<>();
        int activeSlides = (int) Math.ceil(size/slide) + 1;
        memPool = new LinkedList<>();
        for(int i = 0; i < activeSlides; i++){
            memPool.add(new PartialLoadCard());
        }
        lastSlide = memPool.poll();
        this.size = size;
        aggrLoad = 0;
        aggrCard = new BitSet();
    }

    public void updateState(int k){
        lastSlide.load++;
        lastSlide.keySet.set(k);
    }

    public void expireOld(long now){
        PartialLoadCard expiredSlide = activeSlides.peek();
        if (expiredSlide != null && expiredSlide.timestamp <= now - size) {
            activeSlides.poll();
            aggrLoad -= expiredSlide.load;

            aggrCard.clear();
            for (PartialLoadCard p : activeSlides){
                try {
                    aggrCard.or(p.keySet);
                }
                catch (NullPointerException ex){}
            }
            expiredSlide.clear();
            memPool.offer(expiredSlide);
        }
    }

    public void addNewSlide(long ts){
        aggrCard.or(lastSlide.keySet);
        aggrLoad += lastSlide.load;
        lastSlide.timestamp = ts;
        activeSlides.offer(lastSlide);
        lastSlide = memPool.poll();
    }

    public int getLoad(){
        return lastSlide.load + aggrLoad;
    }

    public int getCardinality(){
        BitSet aggr = (BitSet) aggrCard.clone();
        aggr.or(lastSlide.keySet);
        return aggr.cardinality();
    }

    public boolean hasSeen(int key){
        return aggrCard.get(key) || lastSlide.keySet.get(key);
    }
}
