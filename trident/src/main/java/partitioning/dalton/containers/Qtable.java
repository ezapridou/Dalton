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

/**
 * Class representing the Qtable for our contextual bandits implementation
 */
public class Qtable implements Serializable {
    private Map<Integer, QtableEntry> qtable; // key -> qvalues

    public Qtable(){
        qtable = null;
    }

    public Qtable(int numOfKeys){
        qtable = new HashMap<>(numOfKeys);
    }

    public double[] get(int key){
        return qtable.get(key).qvalues;
    }

    public void put(int key, double[] values, int ts){
        qtable.put(key, new QtableEntry(values, ts));
    }

    public boolean containsKey(int key){
        return qtable.containsKey(key);
    }

    public int getExpirationTs(int key){
        return qtable.get(key).expirationTs;
    }

    public void remove(int key){
        qtable.remove(key);
    }

    public void setExpTs(int key, int ts){
        qtable.get(key).expirationTs = ts;
    }

    public boolean isEmpty(){
        return qtable.isEmpty();
    }

    public Map<Integer, QtableEntry> getTable(){
        return qtable;
    }

    public void clear(){
        qtable.clear();
    }

    public int size(){
        return qtable.size();
    }
}
