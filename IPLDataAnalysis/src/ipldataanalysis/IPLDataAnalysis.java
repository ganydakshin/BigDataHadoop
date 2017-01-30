/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis;

/**
 *
 * @author Gany
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IPLDataAnalysis extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf("Three parameters are required for Data Analysis for IPL- <input dir> <intermidiate dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf(), "Job1");
        job.setJarByClass(IPLDataAnalysis.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(DataAnalysisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(DataAnalysisReducer.class);
        job.waitForCompletion(true);

        Job job2 = new Job(getConf(), "Job2");
        job2.setJarByClass(IPLDataAnalysis.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1]+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapperClass(DataAnalysisMapper2.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        
        job2.setReducerClass(DataAnalysisReducer3.class);
        
        

        boolean success = job2.waitForCompletion(true);
        return success ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new IPLDataAnalysis(), args);
        System.exit(exitCode);
    }
}
