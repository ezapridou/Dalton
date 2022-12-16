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

package sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.*;
import java.util.*;

import record.*;

/**
 * Source circular function that continuously feeds the pipeline with data
 * Uses inputs from 2 files
 * To be used when there are 2 sources
 * <p>
 */
// https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.html
public class CircularFeed2files extends RichParallelSourceFunction<Record> {
    private String[] attrs;
    private List<RecordStr> data;
    private boolean cancelled;
    private String filePath1;
    private String filePath2;
    private long timestamp;
    private int sid; // id of the source
    private boolean alternate; // whether to alternate between the 2 files or use only one
    private int step; // step to increase timestamp

    public CircularFeed2files(String[] argPaths, int totalNumOfSources) {
        filePath1 = argPaths[0];
        filePath2 = argPaths[1];
        data = new ArrayList<>();
        cancelled = false;
        timestamp = 0L;
        this.sid = 2;
        this.alternate = false;
        step = totalNumOfSources;
    }

    public CircularFeed2files(String[] argPaths, int id, boolean alternate, int totalNumOfSources) {
        this.sid = id;
        filePath1 = argPaths[0];
        filePath2 = argPaths[1];
        data = new ArrayList<>();
        cancelled = false;
        timestamp = id;
        this.alternate = alternate;
        step = totalNumOfSources;
    }

    @Override
    // Called once during initialization.
    public void open(Configuration conf) throws Exception {
        if (alternate)
            openAlternate();
        else
            openSingleFile();
    }

    /**
     * reads from the two files in an alternating fashion
     * @throws IOException
     */
    private void openAlternate() throws IOException {
        long id = 0L;
        String record;
        try {
            File myObj = new File(filePath1);
            BufferedReader myReader = new BufferedReader(new FileReader(myObj));
            File myObj2 = new File(filePath2);
            BufferedReader myReader2 = new BufferedReader(new FileReader(myObj2));
            while (true) {
                if(id%step==0){
                    record = myReader.readLine();
                    if (record ==null){
                        break;
                    }
                }
                else{
                    record = myReader2.readLine();
                    if (record == null){
                        break;
                    }
                }
                attrs = record.split(",");

                try {
                    data.add(new RecordStr(Integer.parseInt(attrs[0]), attrs[1], id));
                } catch (NumberFormatException e) {
                    System.out.println("Data problem. " + id);
                    e.printStackTrace();
                    continue;
                }
                id++;
            }
            myReader.close();
            myReader2.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred while reading the file.");
            e.printStackTrace();
        }
    }

    /**
     * reads one of the two files depending on sid
     * @throws Exception
     */
    public void openSingleFile() throws Exception {
        long id = 0L;
        String record;
        String filePath = (sid == 1) ? filePath1 : filePath2;
        try {
            File myObj = new File(filePath);
            BufferedReader myReader = new BufferedReader(new FileReader(myObj));
            while ((record = myReader.readLine()) !=null) {
                attrs = record.split(",");
                try {
                    data.add(new RecordStr(Integer.parseInt(attrs[0]), attrs[1], id));
                } catch (NumberFormatException e) {
                    System.out.println("Data problem. " + id);
                    e.printStackTrace();
                    continue;
                }
                id++;
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred while reading the file.");
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
        for (int j = 0; j < data.size() && !cancelled; ) {
            RecordStr record = data.get(j);
            record.setTs(timestamp);

            ctx.collectWithTimestamp(record, timestamp);
            timestamp += step;
            if (j == data.size() - 1) {
                j = 0;
            }
            else{
                j++;
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void close() {
        cancelled = true;
    }
}


