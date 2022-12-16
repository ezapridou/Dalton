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

package partitioning.dalton.state;

import java.util.*;
import java.io.Serializable;

import partitioning.containers.PartialAssignment;
import partitioning.containers.Worker;
import partitioning.dalton.DaltonCooperative;
import partitioning.dalton.containers.HotStatistics;
import record.Record;

/**
 * Class responsible for keeping the necessary state to calculate Dalton's rewards
 */
public class State implements Serializable{

    // track load of each worker
    private List<Worker> workers;

    // track fragmentation in a sliding window
    private Queue<PartialAssignment> memPool;
    private Queue<PartialAssignment> activeSlides;
    private PartialAssignment lastSlide;
    private int aggrLoad;

    private Map<Integer, int[]> keyAssignmentsRefCnt;
    private Map<Integer, BitSet> keyAssignmentsBitSet;

    private int nextUpdate;
    private int slide;
    private int size;
    private int numWorkers;

    private boolean hasSeenHotKeys;

    private HotStatistics hotStatistics;

    private int maxSplit;
    private Set<Integer> hotMaxSplit;


    public State(int size, int slide, int numWorkers, int estimatedNumKeys){
        workers = new ArrayList<>(numWorkers);
        aggrLoad = 0;
        keyAssignmentsBitSet = new HashMap<>(estimatedNumKeys);
        keyAssignmentsRefCnt = new HashMap<>(estimatedNumKeys);
        for(int i = 0; i < numWorkers; i++){
            Worker w = new Worker(size, slide);
            workers.add(w);
        }

        int activeSlides = (int) Math.ceil(size/slide) + 1;
        memPool = new LinkedList<>();
        this.activeSlides = new LinkedList<>();
        for(int i = 0; i < activeSlides; i++){
            memPool.add(new PartialAssignment());
        }
        lastSlide = memPool.poll();

        nextUpdate = slide;

        this.slide = slide;
        this.size = size;
        this.numWorkers = numWorkers;
        this.hasSeenHotKeys = false;

        hotStatistics = new HotStatistics(numWorkers, estimatedNumKeys, slide);

        maxSplit = 2;
        hotMaxSplit = new HashSet<>(numWorkers);
    }

    public int getExpirationTs(){
        return hotStatistics.getExpirationTs();
    }

    private void expireOld(long now){
        PartialAssignment expiredSlide = activeSlides.peek();
        if(expiredSlide != null && expiredSlide.timestamp <= now - size) {
            activeSlides.poll();

            for(Map.Entry<Integer, BitSet> e : expiredSlide.keyAssignment.entrySet()) {
                try {
                    for (int w = 0; w < e.getValue().size(); w++) {
                        if (e.getValue().get(w)) {
                            keyAssignmentsRefCnt.get(e.getKey())[w]--;
                            if (keyAssignmentsRefCnt.get(e.getKey())[w] == 0) {
                                keyAssignmentsBitSet.get(e.getKey()).set(e.getKey(), false);
                            }
                        }
                    }
                } catch (NullPointerException ex){}
            }
            expiredSlide.clear();
            memPool.offer(expiredSlide);
        }
        if (hasSeenHotKeys){
            for(Map.Entry<Integer, BitSet> e : lastSlide.keyAssignment.entrySet()){
                for(int i=0; i<numWorkers; i++){
                    if (e.getValue().get(i)){
                        try {
                            keyAssignmentsRefCnt.get(e.getKey())[i]++;
                        }
                        catch (NullPointerException ex){
                            int[] temp= new int[numWorkers];
                            temp[i] = 1;
                            keyAssignmentsRefCnt.put(e.getKey(), temp);
                        }
                    }
                }
                try {
                    keyAssignmentsBitSet.get(e.getKey()).or(e.getValue());
                }catch (NullPointerException ex){
                    keyAssignmentsBitSet.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    private void addNewSlide(long ts){
        lastSlide.timestamp = ts;
        activeSlides.offer(lastSlide);
        lastSlide = memPool.poll();
    }

    public void updateExpired(Record t, boolean hotKey){ //diff
        if(t.getTs() >= nextUpdate){
            expireOld(t.getTs());
            addNewSlide(nextUpdate);
            for(Worker w : workers){
                aggrLoad -= w.expireOld(t.getTs());
                w.addNewSlide(nextUpdate);
            }
            hasSeenHotKeys = hotKey;
            nextUpdate += slide;
        }
        else if (!hasSeenHotKeys && hotKey){ //first hot key in the slide
            hasSeenHotKeys = true;
        }
    }

    public int isHot(Record tuple, List<DaltonCooperative.Frequency> topKeys){
        return hotStatistics.isHot(tuple, keyAssignmentsBitSet.size(), topKeys);
    }

    public int isHotDAG(Record tuple){
        return hotStatistics.isHotDAG(tuple);
    }

    public void update(Record tuple, int worker){
        updateAssignments(tuple.getKeyId(), worker);
        aggrLoad++;
        workers.get(worker).updateState();
    }

    private void updateAssignments(int key, int worker){
        try {
            lastSlide.keyAssignment.get(key).set(worker);
        }catch (NullPointerException e){
            BitSet b = new BitSet(numWorkers);
            b.set(worker);
            lastSlide.keyAssignment.put(key, b);
        }
    }

    public BitSet keyfragmentation(int key){

        BitSet b = new BitSet(numWorkers);
        try {
            b.or(lastSlide.keyAssignment.get(key));
        }catch (NullPointerException e){
        }
        try {
            b.or(keyAssignmentsBitSet.get(key));
        }catch (NullPointerException e){
        }
        return b;
    }

    public double loadDAG(int selectedWorker){
        final double avgLoad = ((double) aggrLoad)/numWorkers;
        final double L = workers.get(selectedWorker).getLoad();
        return (L - avgLoad)/avgLoad;
    }

    public double avgLoad(){
        return ((double) aggrLoad)/numWorkers;
    }

    public double maxLoad(){
        double maxL = 0;
        double load;
        for (int w=0; w<numWorkers; w++){
            load = getLoad(w);
            if (load > maxL){
                maxL = load;
            }
        }
        return maxL;
    }

    public double getLoad(int worker){
        return workers.get(worker).getLoad();
    }

    public double reward(int key, int selectedWorker, int ch){
        final double kf = keyfragmentation(key).cardinality();
        final double costReduce = kf / numWorkers;

        if (kf == maxSplit){
            hotMaxSplit.add(key);
        }

        final double avgLoad = avgLoad();
        final double L = getLoad(selectedWorker);
        final double costMap = (L - avgLoad)/Math.max(L,avgLoad);

        if (hotMaxSplit.size() >= 0.7*ch  && maxSplit < 6){
            hotMaxSplit.clear();
            maxSplit++;
        }

        return -(0.5 * costMap + 0.5 * costReduce);
    }

    public boolean inHotMax(int keyId){
        return hotMaxSplit.contains(keyId);
    }

    public void removeFromHotMax(int keyId){
        hotMaxSplit.remove(keyId);
    }

    public int getTotalCountOfRecords(){
        return hotStatistics.getTotal();
    }

    public void setFrequencyThreshold(int t){
        hotStatistics.setFrequencyThreshold(t);
    }

    public void setHotInterval(int h){
        hotStatistics.setHotInterval(h);
    }
}
