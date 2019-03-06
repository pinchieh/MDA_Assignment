package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

    public static class Map1 extends Mapper<Object, Text, Text, Text>{


    Text k = new Text();
    Text v = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
        k.set(itr.nextToken());
        v.set(itr.nextToken());
        }
        context.write(k, v);
    }
  }
    public static class Red1 extends Reducer<Text,Text,Text,Text> {


      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          double nodes_num = 10876.0;
          double initial_rank = (1/nodes_num);
          int judge = 0;
          String links = String.valueOf(initial_rank) + ">";
          for (Text value : values) {
              if (judge!=0)
                  links += ",";
              links += value.toString();
              judge = 1;
          }
          context.write(key, new Text(links));

      }
  }


  public static class Map2 extends Mapper<Object, Text, Text, Text>{

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

              double rank = 0.0;
              String str = value.toString();
              String[] scrnode = str.split("\t");
              String node = scrnode[0];
              String[] pr_link = scrnode[1].split(">");
              String outlink = "";
              String[] outpage = {};
              if (pr_link.length>1)  // see if it has outlink or not
              {
                outlink = pr_link[1];
                context.write(new Text(node),new Text(outlink));
                outpage = outlink.split(",");
              }

              for (String op : outpage){
                  rank = Double.valueOf(pr_link[0])/ outpage.length;
                  context.write(new Text(op),new Text(String.valueOf(rank)));
              }
      }

  }
  public static class Red2 extends Reducer<Text,Text,Text,Text> {


     public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          double pgrank = 0.0;
          double beta = 0.8;
          String outlink_list = "";
          for (Text pr_urls : values){

              if (pr_urls.toString().indexOf(".") != -1)  //get pagerank value
              {
                String str = pr_urls.toString().substring(0, pr_urls.toString().length());
                pgrank += Double.valueOf(str.toString());
              }
              else
                outlink_list = pr_urls.toString() ;

          }

          pgrank = (1 - beta)/10876 + (beta * pgrank ) ;


          String backterm = String.valueOf(pgrank)+">";
          backterm += outlink_list ;

          context.write(key , new Text(backterm));

          /*for (Text pr_urls : values){
              context.write(key , pr_urls);
          }*/

    }
  }
  public static class Map3 extends Mapper<Object, Text, Text, Text>{


  //map to the same key to store the initial value
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] initialmap = str.split("\t");
            String s = "";
            s = s + initialmap[0] + ">" + initialmap[1];
            String k = "S";
            context.write(new Text(k), new Text(s));
      }

  }
  public static class Red3 extends Reducer<Text,Text,Text,Text> {

//renormalize the pagerank value
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

          double pagerank_sum = 0.0;
          double newrank = 0.0;
          String tmp = "";
          for(Text value : values)
          {
            String str = value.toString();
            tmp += str+"\t";
            String[] val = str.split(">");
            pagerank_sum += Double.valueOf(val[1]); // sum all pagerank
          }
          String[] allvalue = tmp.split("\t");

          for(String all : allvalue)
          {
              String[] line =  all.split(">");
              String node = line[0];
              String prank = line[1];
              String outnode = "";
              if(line.length > 2)
                  outnode = line[2];
              newrank = Double.valueOf(prank) + (1-pagerank_sum)/ 10876 ;
              prank = String.valueOf(newrank);

              context.write(new Text(node) , new Text(prank+">"+outnode));
          }



          /*for(Text pr_urls1: values )
          {
              context.write(key ,pr_urls1);
          }*/
      }
  }

 public static class Map4 extends Mapper<Object, Text, Text, Text>{

    // map to the same key to store the initial value

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] initialmap = str.split("\t");
            String s = "";
            s = s + initialmap[0] + ">" + initialmap[1];
            String k = "S";
            context.write(new Text(k), new Text(s));
      }

  }
  public static class Red4 extends Reducer<Text,Text,Text,Text> { // sort by value using treeMap
      private Text outputkey = new Text();
      private Text outputvalue = new Text();

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

          Map<Double,String> map = new TreeMap<Double,String>(Collections.reverseOrder());
          String tmp = "";
          for(Text value : values)
          {
            String str = value.toString();
            tmp += str+"\t";
            String[] val = str.split(">");
          }
          String[] allvalue = tmp.split("\t");
          for(String all : allvalue)
          {
              String[] line =  all.split(">");
              String node = line[0];
              String prank = line[1];
              String outnode = "";
              if(line.length > 2)
                  outnode = line[2];
              map.put(Double.valueOf(prank),node);
          }
          int i = 0;
          for(Map.Entry<Double,String> entry : map.entrySet())
          {
              if(i>=10)
                break;
              outputkey.set(entry.getValue());
              outputvalue.set(Double.toString(entry.getKey()));
              context.write(outputkey,outputvalue);
              i++;
          }

          /*for(Text pr_urls1: values )
          {
              context.write(key ,pr_urls1);
          }*/
      }
  }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
          System.err.println("Usage: PageRank <in> <out>");
          System.exit(2);
      }
      Job job = new Job(conf, "PageRank");
      job.setJarByClass(PageRank.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Map1.class);
      job.setReducerClass(Red1.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_0"));

      job.waitForCompletion(true);

      int iterk = 0;
      int iterp = 1;
      int iteru = 2;

      String k = "";
      String p = "";
      String u = "";

      for(int j = 0 ; j < 20 ; j++)
      {
        k = String.valueOf(iterk);
        p = String.valueOf(iterp);
        u = String.valueOf(iteru);

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Map2");
        job2.setJarByClass(PageRank.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Red2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path("output/out_"+k));
        FileOutputFormat.setOutputPath(job2, new Path("output/out_"+p));

        job2.waitForCompletion(true);


        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Map3");
        job3.setJarByClass(PageRank.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapperClass(Map3.class);
        job3.setReducerClass(Red3.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path("output/out_"+p));
        FileOutputFormat.setOutputPath(job3, new Path("output/out_"+u));

        job3.waitForCompletion(true);

        iterk += 2;
        iterp += 2;
        iteru += 2;

      }

      Configuration conf4 = new Configuration();
      Job job4 = new Job(conf4, "Map4");
      job4.setJarByClass(PageRank.class);
      job4.setOutputKeyClass(Text.class);
      job4.setOutputValueClass(Text.class);
      job4.setMapperClass(Map4.class);
      job4.setReducerClass(Red4.class);
      job4.setInputFormatClass(TextInputFormat.class);
      job4.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job4, new Path("output/out_"+u));
      FileOutputFormat.setOutputPath(job4, new Path("output/out_50"));

      job4.waitForCompletion(true);


      }

}
