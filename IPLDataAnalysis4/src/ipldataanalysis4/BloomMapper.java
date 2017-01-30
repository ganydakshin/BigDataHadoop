/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis4;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Gany
 */
public class BloomMapper extends Mapper<Object, Text, Text, NullWritable> {

    Funnel<Scores> p = new Funnel<Scores>() {

        @Override
        public void funnel(Scores scores, Sink ps) {
            ps.putString(scores.getName(), Charsets.UTF_8);
        }

    };
    BloomFilter<Scores> friends = BloomFilter.create(p, 500, 0.1);

    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException {

        Scores s1 = new Scores("MS Dhoni");
        //Scores s2 = new Scores(2, "MS Dhoni", 6);
        Scores s3 = new Scores("SK Raina");
        ArrayList<Scores> listScores = new ArrayList<>();
        listScores.add(s1);
        //listScores.add(s2);
        listScores.add(s3);
        for(Scores s : listScores) {
            friends.put(s);
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (value != null) {
            String arrAttributes[] = value.toString().split(",");
            if (arrAttributes != null) {
                if (!arrAttributes[0].equals("match_id")) {
                    //int total_runs = Integer.parseInt(String.valueOf(arrAttributes[17]));
                    //int extra_runs = Integer.parseInt(String.valueOf(arrAttributes[16]));
                    //int sc = total_runs - extra_runs;
                    String playerName = arrAttributes[6];
                    int match_id = Integer.parseInt(String.valueOf(arrAttributes[0]));
                    Scores s = new Scores(playerName);
                    if (friends.mightContain(s)) {
                        //IntWritable sco = new IntWritable(sc);
                        String st = playerName + " " + value.toString();
                        Text v = new Text(st);
                        context.write(v, NullWritable.get());
                    }
                }
            }
        }

    }

}
