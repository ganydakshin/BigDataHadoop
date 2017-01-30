/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class IPLDataAnalysis2 extends Configured implements Tool{

    /**
     * @param args the command line arguments
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Two parameters are required for Data Analysis for IPL- <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf(), "Job1");
        job.setJarByClass(IPLDataAnalysis2.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        MultipleOutputs.addNamedOutput(job,"bins",TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        job.setMapperClass(DataAnalysisMapper.class);
        job.setNumReduceTasks(0);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new IPLDataAnalysis2(), args);
        System.exit(exitCode);
    }
    
}
