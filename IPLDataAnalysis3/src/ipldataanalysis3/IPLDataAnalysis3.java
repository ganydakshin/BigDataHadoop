/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
public class IPLDataAnalysis3 extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new Configuration(),new IPLDataAnalysis3(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Two parameters are required for Data Analysis for IPL- <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf(), "Job1");
        job.setJarByClass(IPLDataAnalysis3.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DataAnalysisMapper.class);
        job.setNumReduceTasks(13);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(DataAnalysisPartitioner.class);
        job.setReducerClass(DataAnalysisReducer.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
    
}
