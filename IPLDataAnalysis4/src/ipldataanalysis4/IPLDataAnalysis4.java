/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis4;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Gany
 */
public class IPLDataAnalysis4 extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new Configuration(),new IPLDataAnalysis4(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Two parameters are required for Data Analysis for IPL- <input dir> <output dir>\n");
            return -1;
        }
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI("testFile"), conf);
        Job job = new Job(getConf(), "Job1");
        long milliSeconds = 1000*60*60;
        conf.setLong("mapred.task.timeout", milliSeconds);
        
        job.setJarByClass(IPLDataAnalysis4.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BloomMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        job.setNumReduceTasks(0);
        //job.setReducerClass(DataAnalysisReducer.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
    
}
