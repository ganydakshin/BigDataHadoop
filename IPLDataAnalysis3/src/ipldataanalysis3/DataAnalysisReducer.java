/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Gany
 */
public class DataAnalysisReducer extends Reducer<Text,IntWritable,Text,Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i : values) {
            sum+=Integer.parseInt(String.valueOf(i));
        }
        context.write(key,new Text(String.valueOf(sum)));
    }
    
}
