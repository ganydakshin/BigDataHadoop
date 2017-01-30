/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.BooleanWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Gany
 */
public class DataAnalysisReducer3 extends Reducer<LongWritable, Text, Text, LongWritable> {

    private static int count = 0;


    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values) {
            if (count < 10) {
                count++;
                context.write(new Text(t), key);
            }
        }
    }
}
