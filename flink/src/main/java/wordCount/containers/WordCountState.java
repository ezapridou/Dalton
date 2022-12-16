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

package wordCount.containers;

import java.util.*;

/**
 * Class/struct used for the state of a word count worker
 */
public class WordCountState {
    private int key;
    private Map<String, Integer> counts;

    private boolean isHot; // used for key forwarding

    public WordCountState(int key){
        this.key = key;
        counts = new HashMap<>();
    }

    public int getKey(){
        return key;
    }

    public void countInc(String s){
        countInc(s, 1);
    }

    public void countInc(String s, int c){
        if (counts.containsKey(s)){
            counts.put(s, counts.get(s) + c);
        }
        else{
            counts.put(s, c);
        }
    }

    public Map<String, Integer> getCounts() {
        return counts;
    }

    public int getCount(String s){
        return counts.get(s);
    }

    public void setHot(boolean hot){
        isHot = hot;
    }

    public boolean isHot(){
        return isHot;
    }

    public String toString(){
        return counts.toString();
    }
}