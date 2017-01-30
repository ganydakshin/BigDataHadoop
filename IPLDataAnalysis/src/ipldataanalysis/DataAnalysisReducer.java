/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Gany
 */
public class DataAnalysisReducer extends Reducer<Text, Text, Text, Text> {

    //static HashMap<Text, Integer> hm = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text t : values) {
            count++;
        }
        context.write(key, new Text(String.valueOf(count)));
    }

}
