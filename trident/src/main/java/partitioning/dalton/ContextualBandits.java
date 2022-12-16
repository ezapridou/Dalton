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

import java.io.Serializable;
import java.util.BitSet;
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

    public boolean isHot(int keyId, int ts){
        boolean isHot = true;
        int result = state.isHot(keyId, ts);  // returns 0 if not hot, 1 if hot, expirationTs if it just became hot
        if (result == 0){ // not hot in current window
            try{
                if (ts > qtable.getExpirationTs(keyId)){ // hot in previous window (expired)
                    qtable.remove(keyId);
                    isHot = false;
                }
            }
            catch (NullPointerException ex){ //key not in qtable
                isHot = false;
            }
        }
        else if (result != 1){ // key just added to this window's hot keys
            if (!qtable.containsKey(keyId)) {
                initializeKey(keyId, result);
            }
            else{
                qtable.setExpTs(keyId, result);
            }
        }

        // if result == 1 then key was already hot in current window
        return isHot;
    }

    public void expireState(int ts, boolean isHot){
        state.updateExpired(ts, isHot);
    }

    public int partition(int keyId, boolean isHot){
        return (isHot)?partitionHot(keyId):hash(keyId);
    }

    public void updateState(int keyId, int worker){
        state.update(keyId, worker);
    }

    public double updateQtable(int keyId, boolean isHot, int worker){
        if(isHot) {
            double reward =  state.reward(keyId, worker, qtable.size());
            update(keyId, worker, reward);
            return reward;
        }
        return -3;
    }

    public int partitionHot(int keyId) {
        int worker = hash(keyId);
        double r = rnd.nextDouble();
        if(r < 1.0 - epsilon) {  // exploitation
            try {
                double max = qtable.get(keyId)[worker];
                for (int i = 0; i < numOfWorkers; i++) {
                    if (qtable.get(keyId)[i] > max) {
                      max = qtable.get(keyId)[i];
                      worker = i;
                    }
                 }
            } catch(NullPointerException ex){ // here it means that the key was found hot before we got an updated qtable from master
                initializeKey(keyId, state.getExpirationTs());
                return worker;
            }
        }else{ // exploration
            int rndI;
            if (state.inHotMax(keyId)){

                BitSet b = state.keyfragmentation(keyId);
                int bcar = b.cardinality();
                if ( bcar == 0){
                    state.removeFromHotMax(keyId); //key removed from the set of hot from QTableReducer
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

}
