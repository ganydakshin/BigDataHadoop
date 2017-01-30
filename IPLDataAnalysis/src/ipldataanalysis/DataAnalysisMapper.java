/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Gany
 */
public class DataAnalysisMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value != null) {
            
            String arrAttributes[] = value.toString().split(",");
            if (arrAttributes != null & arrAttributes.length > 19) {
                if (!arrAttributes[0].equals("match_id") && !arrAttributes[18].equals("")) {
                    if (arrAttributes[18] != null) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(arrAttributes[18]).append(",").append(arrAttributes[8]);
                        Text playersInvolved = new Text(sb.toString());
                        context.write(playersInvolved, value);
                    }
                }
            }

        }

    }
}
