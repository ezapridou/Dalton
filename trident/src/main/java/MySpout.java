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

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySpout implements IBatchSpout {
    int index;
    Fields fields;
    int batchSize;
    List<List<Values>> batches;
    Map<Long, List<Values>> createdBatches;
    boolean cycle;
    Logger LOG;
    String file;
    int step;

    public MySpout(Fields fields, int batchSize, Logger LOG, String file, int id, int step){
        this.fields = fields;
        this.batchSize = batchSize;
        this.LOG = LOG;
        this.file = file;
        cycle = false;
        this.index = id;
        this.step = step;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context){
        createdBatches = new HashMap<>();
        batches = new ArrayList<>();
        readData(file);
    }

    public void readData(String file){
        String record;
        String[] attrs;
        int total = 1000000; // number of tuples to be read, tune it acc. to how many tuples exist and the memory size
        List<Values> currentBatch = new ArrayList<>();
        int i=0;
        try {
            File myObj = new File(file);
            BufferedReader myReader = new BufferedReader(new FileReader(myObj));
            while ((record = myReader.readLine()) != null && total > i) {
                if (i != 0 && i % batchSize == 0){
                    batches.add(currentBatch);
                    currentBatch = new ArrayList<>();
                }
                attrs = record.split(",");
                try{
                    currentBatch.add(new Values(Integer.parseInt(attrs[0]), attrs[1]));
                    i++;
                } catch (NumberFormatException e) {
                    LOG.info("Data problem.");
                    e.printStackTrace();
                    continue;
                }
            }
            myReader.close();
        } catch (FileNotFoundException e){
            LOG.info("An error occured while reading the file");
            e.printStackTrace();
        } catch (java.io.IOException e) {
            LOG.info("Problem while reading data");
            e.printStackTrace();
        }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector){
        List<Values> batch = createdBatches.get(batchId);
        if (batch == null){
            if (index >= batches.size()) {
                if (cycle) {
                    index = 0;
                }
                else{
                  return;
                }
            }
            batch = batches.get(index);
            createdBatches.put(batchId, batch);
            index += step;
        }
        for (Values tuple : batch){
            collector.emit(tuple);
        }
    }

    @Override
    public void ack(long batchId){
        batches.remove(batchId);
    }

    @Override
    public void close(){

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
}
