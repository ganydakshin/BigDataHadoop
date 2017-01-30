/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Gany
 */
public class DataAnalysisMapper extends Mapper<Object,Text,Text,IntWritable> {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if(value != null) {
            String arrAttributes[] = value.toString().split(",");
            if (arrAttributes != null) {
                if (!arrAttributes[0].equals("match_id")) {
                    StringBuilder sb = new StringBuilder();
                    IntWritable score = new IntWritable(Integer.parseInt(arrAttributes[17]) - Integer.parseInt(arrAttributes[16]));
                    
                    sb.append(arrAttributes[2]).append(",").append(arrAttributes[6]).append(",");
                    Text playersInvolved = new Text(sb.toString());
                    
                    context.write(playersInvolved, score);
                } 
            }
        }
    }
}
