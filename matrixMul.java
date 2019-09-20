package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import java.util.*;

public class matrixMul {

    public static class Map1 extends Mapper<Object, Text, Text, Text>{


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
         String[] line = value.toString().split(",");
         String mapval = "";
         String matrixID = line[0];
         String posval = line[3];
         String jpos = "";
         if(matrixID.equals("M"))
         {
             jpos = line[2];
             String ipos = line[1];
             mapval = matrixID+","+ipos+","+posval;
             context.write(new Text(jpos), new Text(mapval));
         }
         else
         {
             jpos = line[1];
             String kpos = line[2]; 
             mapval = matrixID+","+kpos+","+posval;
             context.write(new Text(jpos), new Text(mapval));
         }
         
        }
    }


    public static class Red1 extends Reducer<Text,Text,Text,Text> {
    
    
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        ArrayList<Integer> MList = new ArrayList<>();
        ArrayList<Integer> NList = new ArrayList<>();
        for(Text val : values)
        {
            String[] line = val.toString().split(",");
            String matrixID = line[0];
            int ipos = Integer.valueOf(line[1]);
            int eleval = Integer.valueOf(line[2]);
            if(matrixID.equals("M"))
            {
                MList.add(ipos);
                MList.add(eleval);
            }
            else
            {
                NList.add(ipos);
                NList.add(eleval);
            }
        
        }
        int Mlen = MList.size() , Nlen = NList.size();
        for(int i = 0 ; i < Mlen ; i+=2)
        {
            for(int j = 0 ; j < Nlen ; j+=2)
            {
                String i_k_pos = "";
                i_k_pos = String.valueOf(MList.get(i))+","+String.valueOf(NList.get(j));
                String product = String.valueOf(MList.get(i+1) * NList.get(j+1));
                context.write(new Text(i_k_pos) , new Text(product));
            }
        }
    }
}
public static class Map2 extends Mapper<Object, Text, Text, Text>{


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] line = value.toString().split("\t");
             context.write(new Text(line[0]) , new Text(line[1]));
        }
    }


    public static class Red2 extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text val : values)
            {
                String product = val.toString();
                sum += Integer.valueOf(product);
            }
            context.write(key , new Text(String.valueOf(sum)));
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "matrixMul");
    job1.setJarByClass(matrixMul.class);
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Red1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("data/test.txt"));
    FileOutputFormat.setOutputPath(job1, new Path("output/matrixMul_1"));
    job1.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "matrixMul");
    job2.setJarByClass(matrixMul.class);
    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Red2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("output/matrixMul_1"));
    FileOutputFormat.setOutputPath(job2, new Path("output/matrixMul_2"));
    job2.waitForCompletion(true);
    
    
    }
}
