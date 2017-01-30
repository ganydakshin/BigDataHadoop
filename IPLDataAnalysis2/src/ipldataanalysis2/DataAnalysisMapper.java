/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis2;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author Gany
 */
public class DataAnalysisMapper extends Mapper<Object, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> mos = null;

    protected void setup(Context context) {
        // Create a new MultipleOutputs using the context object
        mos = new MultipleOutputs(context);
    }

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            if (value.toString().length() > 0) {
                String records[] = value.toString().split(",");

                if (!(records[0].equals("match_id"))) {

                    HashMap<String, Integer> recordFilter = new HashMap<String, Integer>();
                    recordFilter.put(records[2], 1);

                    for (String s : recordFilter.keySet()) {
                        if (records[2].equals(s)) {
                            mos.write("bins", value, NullWritable.get(), s);
                            break;
                        }
                    }

                }

            }
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException  {
        mos.close();
    }
}
