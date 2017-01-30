/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Gany
 */
public class DataAnalysisMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value != null) {
            String arrAttributes[] = value.toString().split("\\t");
            context.write(new LongWritable(Long.parseLong(arrAttributes[1])), new Text(arrAttributes[0]));
        }

    }

}
