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

import java.util.*;

public class Kmeans {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {////// read in and judge data point or cen1 or cen2
       int dataNode = 1;
       int cenNode = 0;
       if (flag.substring(0,1).equals("d"))
           context.write(new Text("S") , new Text(Integer.toString(dataNode)+ "," + value.toString()));
       else
           context.write(new Text("S") , new Text(Integer.toString(cenNode)+ "," + value.toString()));
  }
}
  public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int data_ID = 1;
            int cent_ID = -1;
            for(Text val : values)
            {
              String[] lines = val.toString().split(",");
              if(Integer.valueOf(lines[0]) == 1)
              {
                context.write(new Text(Integer.toString(data_ID)) , new Text(lines[1]));
                data_ID++;
              }
              else
              {
                context.write(new Text(Integer.toString(cent_ID)) , new Text(lines[1]));///////////if id = 0 then is centroid
                cent_ID--;
              }
            }

        }
    }
  public static class Mapper2 extends Mapper<Object, Text, Text, Text>{

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int node_num = 4601;
        String[] lines = value.toString().split("\t");
        if(!lines[0].equals("#"))
        {
            if(Integer.valueOf(lines[0])> 0)//ID > 0 then data point
                context.write(new Text(lines[0]) , new Text(lines[0] + "," + lines[1]));//    < 1,  (1,feature)>..<2 , (2,feature)....etc >
            else
            {
                for(int i = 1 ; i <= node_num ; i++)
                    context.write(new Text(Integer.toString(i)) , new Text(lines[0] + "," + lines[1])); // <1 , (centroid ID_1 (-1) , cen_feature_1)> , <2 , (centroid ID_1 (-2) , cen_feature_2)>
            }
        }
    }
}

  public static class Reducer2 extends Reducer<Text,Text,Text,Text> {    ///

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        int feature_dim = 58;
        ArrayList<String> cen_list = new ArrayList<String>();
        String node = "";// position
        double min_dist = Double.MAX_VALUE;
        String clus_ID = "";
        for(Text val : values)
        {
          String[] lines = val.toString().split(",");
          if(Integer.valueOf(lines[0]) > 0 )
              node = val.toString();
          else
              cen_list.add(val.toString());
        }

        for(String centroid : cen_list)
        {
            String[] node_feature = node.split(",")[1].split(" ");
            String[] cen = centroid.split(",");
            String[] cen_feature = cen[1].split(" ");
            double dist = 0.0;
            for(int i = 0 ; i < feature_dim ; i++)
            {
              dist += Math.pow(Double.valueOf(node_feature[i]) - Double.valueOf(cen_feature[i]) , 2);
              //dist += Math.abs(Double.valueOf(node_feature[i]) - Double.valueOf(cen_feature[i]));

            }
            dist = Math.sqrt(dist);
            if(dist < min_dist)
            {
              min_dist = dist;
              clus_ID = cen[0];
            }
        }
        context.write(new Text("#") , new Text(Double.toString(min_dist)));
        context.write(new Text(clus_ID) , new Text(node)); //ex: <-1 , (1, feature)>


  }
}
   public static class Mapper3 extends Mapper<Object, Text, Text, Text>{



    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

       String[] lines = value.toString().split("\t");
       context.write(new Text(lines[0]), new Text(lines[1]));

  }
}
  public static class Reducer3 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          int feature_dim = 58;
          if(key.toString().equals("#"))
          {
            double dist  = 0.0;
            for(Text val: values)
            {
                dist += Math.pow(Double.valueOf(val.toString()),2);//Euclidean's cost function in hw
                // dist += Double.valueOf(val.toString());//Manhatton's dist cost function in hw
            }
            context.write(new Text("#") , new Text(Double.toString(dist)));
          }
          else
          {
            double[] total = new double[feature_dim];
            for(int i = 0 ; i< feature_dim ;i++ )
                total[i] = 0.0;
            int node_num = 0;

            for(Text val :values )
            {
              String[] lines = val.toString().split(",");
              String[] feature = lines[1].split(" ");
              for(int i = 0 ; i < feature_dim ; i++)
                  total[i] += Double.valueOf(feature[i]);

              node_num++;
              context.write(new Text(lines[0]),new Text(lines[1]));// write data point

            }

            String new_cen = "";
            for(int i = 0 ; i < feature_dim ; i++)
            {
              total[i] /= node_num;   //  how much node in cluster then the average sum of position is the new centriod
              new_cen+=Double.toString(total[i])+ " ";
            }
            new_cen = new_cen.substring(0 , new_cen.length()-1);

            context.write(key , new Text(new_cen));
          }


        }
    }

public static void main(String[] args) throws Exception {

    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "Kmeans");
    job1.setJarByClass(Kmeans.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    //FileInputFormat.addInputPath(job1, new Path("data/c1.txt"));
    FileInputFormat.addInputPath(job1, new Path("data/c2.txt"));
    FileInputFormat.addInputPath(job1, new Path("data/data.txt"));
    FileOutputFormat.setOutputPath(job1, new Path("output/kmeans@@@@_1"));
    job1.waitForCompletion(true);

    for(int i = 0 ; i < 20 ; i++)
    {
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Kmeans");
        job2.setJarByClass(Kmeans.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("output/kmeans@@@@_" + Integer.toString(2 * i + 1)));
        FileOutputFormat.setOutputPath(job2, new Path("output/kmeans@@@@_" + Integer.toString(2 * i + 2)));
        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Kmeans");
        job3.setJarByClass(Kmeans.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("output/kmeans@@@@_" + Integer.toString(2 * i + 2)));
        FileOutputFormat.setOutputPath(job3, new Path("output/kmeans@@@@_" + Integer.toString(2 * i + 3)));
        job3.waitForCompletion(true);
    }

    }
}
