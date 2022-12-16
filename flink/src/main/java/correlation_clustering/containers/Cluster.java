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

import record.Record;
import record.RecordStr;

import java.util.*;

/**
 * Just an abstraction for a cluster
 *
 * holds a set with the records of the cluster
 */
public class Cluster{
    private final long INIT_ID = 999;

    protected Set<RecordStr> records;
    protected long id = INIT_ID; //the id of the first record of the cluster becomes the cluster id

    // constructor
    public Cluster(){
        records = new HashSet<>();
    }

    // constructor (initialize with record and id)
    public Cluster(long id, RecordStr record){
        records = new HashSet<>();
        records.add(record);
        this.id = id;
    }

    // constructor (initialize)
    public Cluster(long id){
        this.id = id;
        records = new HashSet<>();
    }

    // constructor (initialize with record)
    public Cluster(RecordStr record){
        records = new HashSet<>();
        records.add(record);
        id = record.getTs();
    }

    public long getId() { return id; }

    public Set<RecordStr> getRecords(){
        return records;
    }

    public void add(RecordStr record){
        if (id == INIT_ID){
            id = record.getTs();
        }
        records.add(record);
    }

    public void remove(RecordStr record){
        records.remove(record);
    }

    public void addAll(Cluster c){
        if (c.size()>0 && id == INIT_ID){
            for (Record t : c.getRecords()){
                id = t.getTs();
                break;
            }
        }
        records.addAll(c.getRecords());
    }

    public int size(){
        return records.size();
    }

    public String toString(){
        return records.toString();
    }
}
