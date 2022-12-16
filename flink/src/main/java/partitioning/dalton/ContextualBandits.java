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

package partitioning.dalton;

import partitioning.dalton.containers.Qtable;
import partitioning.dalton.state.State;
import record.Record;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class ContextualBandits implements Serializable {
    private int INIT_VAL = -2; // initial value of an action
    private Qtable qtable;
    private double a; // constant a for non-stationary problem
    private double epsilon; //detrmines exploration/expoitation ratio
    private Random rnd;
    private final int seed = 3;

    private State state;

    private int numOfWorkers;

    public ContextualBandits(int numOfWorkers, int slide, int size, int numOfKeys){
        this.numOfWorkers = numOfWorkers;
        rnd = new Random(seed);
        this.a = 0.1;
        this.epsilon = 0.1;
        qtable = new Qtable(numOfKeys);

        state = new State(size, slide, numOfWorkers, numOfKeys);
    }

    public int hash(int key){
        return (key) % numOfWorkers;
    }

    // used for debugging purposes
    public State getState(){
        return state;
    }

    public Qtable getQtable(){
        return qtable;
    }

    public void setQtable(Qtable newQtable){
        qtable = newQtable;
    }

    public boolean isHot(Record r, List<DaltonCooperative.Frequency> topKeys){
        boolean isHot = true;
        int result = state.isHot(r, topKeys);  // returns 0 if not hot, 1 if hot, expirationTs if it just became hot
        if (result == 0){ // not hot in current window
            try{
                if (r.getTs() > qtable.getExpirationTs(r.getKeyId())){ // hot in previous window (expired)
                    qtable.remove(r.getKeyId());
                    isHot = false;
                }
            }
            catch (NullPointerException ex){ //key not in qtable
                isHot = false;
            }
        }
        else if (result != 1){ // key just added to this window's hot keys
            if (!qtable.containsKey(r.getKeyId())) {
                initializeKey(r.getKeyId(), result);
            }
            else{
                qtable.setExpTs(r.getKeyId(), result);
            }
        }

        // if result == 1 then key was already hot in current window
        return isHot;
    }

    public boolean isHot(Record r){
        return isHot(r, null);
    }

    public void expireState(Record r, boolean isHot){
        state.updateExpired(r, isHot);
    }

    public int partition(Record r, boolean isHot){
        return (isHot)?partitionHot(r):hash(r.getKeyId());
    }

    public void updateState(Record r, int worker){
        state.update(r, worker);
    }

    public double updateQtable(Record r, boolean isHot, int worker){
        if(isHot) {
            double reward =  state.reward(r.getKeyId(), worker, qtable.size());
            update(r.getKeyId(), worker, reward);
            return reward;
        }
        return -3;
    }

    public int partitionHot(Record t) {
        int worker = hash(t.getKeyId());
        double r = rnd.nextDouble();
        if(r < 1.0 - epsilon) {  // exploitation
            try {
                double max = qtable.get(t.getKeyId())[worker];
                for (int i = 0; i < numOfWorkers; i++) {
                    if (qtable.get(t.getKeyId())[i] > max) {
                      max = qtable.get(t.getKeyId())[i];
                      worker = i;
                    }
                 }
            } catch(NullPointerException ex){ // here it means that the key was found hot before we got an updated qtable from master
                initializeKey(t.getKeyId(), state.getExpirationTs());
                return worker;
            }
        }else{ // exploration
            int rndI;
            if (state.inHotMax(t.getKeyId())){

                BitSet b = state.keyfragmentation(t.getKeyId());
                int bcar = b.cardinality();
                if ( bcar == 0){
                    state.removeFromHotMax(t.getKeyId()); //key removed from the set of hot from QTableReducer
                    worker = rnd.nextInt(numOfWorkers);
                    return worker;
                }
                rndI = rnd.nextInt(bcar);
                int j=-1;
                for (int i=0; i<b.size(); i++){
                    if (b.get(i)){
                        j++;
                    }
                    if (j==rndI){
                        worker = i;
                        break;
                    }
                }
            }
            else{
                worker = rnd.nextInt(numOfWorkers);
            }
        }
        return worker;
    }

    public void update(int key, int action, double reward) {
        try {
            qtable.get(key)[action] = qtable.get(key)[action] * (1 - a) + a * reward;
        } catch (NullPointerException ex){ // here it means that the key was found hot before we got an updated qtable from master
            if (qtable == null){
                qtable = new Qtable();
            }
            initializeKey(key, state.getExpirationTs());
            update(key, action, reward);
        }
    }

    private void initializeKey(int key, int expTs){
        double[] actions = new double[numOfWorkers];
        for(int j = 0; j < numOfWorkers; j++){
            actions[j] = INIT_VAL;
        }
        qtable.put(key, actions, expTs);
    }

    public int getTotalCountOfRecords(){
        return state.getTotalCountOfRecords();
    }

    public void setFrequencyThreshold(int t){
        state.setFrequencyThreshold(t);
    }

    public void setHotInterval(int h){
        state.setHotInterval(h);
    }
}
