/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ipldataanalysis3;

import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author Gany
 */
public class DataAnalysisPartitioner extends Partitioner<Text, IntWritable> {
   
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String[] arr = key.toString().split(",");
        String team = arr[0];
        if(team.equalsIgnoreCase("Royal Challengers Bangalore")) {
            return 1 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Chennai Super Kings")) {
            return 2 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Deccan Chargers")) {
            return 3 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Delhi Daredevils")) {
            return 4 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Kolkata Knight Riders")) {
            return 5 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Rajasthan Royals")) {
            return 6 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Mumbai Indians")) {
            return 7 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Gujarat Lions")) {
            return 8 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Kings XI Punjab")) {
            return 9 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Rising Pune Supergiants")) {
            return 10 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Kochi Tuskers Kerala")) {
            return 11 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Sunrisers Hyderabad")) {
            return 12 % numReduceTasks;
        }
        else if(team.equalsIgnoreCase("Pune Warriors")) {
            return 13 % numReduceTasks;
        }
        else {
            return 0;
        }
        //return (team.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        
    }
}
