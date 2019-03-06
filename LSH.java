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


public class LSH {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit(); // get file name
            flag = split.getPath().getName();
    }


    private final int k = 3; // k-gram

    Text shingle = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String doc = value.toString();
        doc = doc.replaceAll("\\.", "");
        doc = doc.replaceAll(",", "");
        doc = doc.replaceAll("\\-", "");
        doc = doc.replaceAll("\"", "");
        doc = doc.replaceAll("\\s+"," ");

        String[] doc_split = doc.split(" ");

        for(int i = 0 ; i < doc_split.length - k ; i++)
        {
            String s = "";
            for(int j = i ; j < i + k ; j++)
                s = s + doc_split[j] + " ";
            shingle.set(s);
            context.write(shingle, new Text("D_" + flag.substring(0, 3)));
        }
    }
  }

public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          ArrayList<String> docs = new ArrayList<String>();
          for (Text value : values) {
              if(!docs.contains(value.toString())) // if the words does not appear in docs then add it into arraylist
                docs.add(value.toString());
          }

          String doc_list = "";
          for(String doc : docs)
            doc_list = doc_list + doc + ",";

          context.write(key, new Text(doc_list));
      }
  }



  public static class Mapper2 extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Map to same key to save initial format
            String str = value.toString();
            String[] initialmap = str.split("\t");
            String s = "";
            s = s + initialmap[0] + ">" + initialmap[1];
            String k = "S";
            context.write(new Text(k), new Text(s));


        }
    }
    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
              int index = 1;
              for(Text value : values) // caculate how many shingles there are
              {
                  String str = value.toString();
                  String[] word_doc = str.split(">");
                  context.write(new Text(String.valueOf(index)),new Text(word_doc[1]));
                  index += 1;
              }
          }
      }

   public static class Mapper3 extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             int prime = 12569;
             int N = 12371;
             int hash_output = 0;
             String str = value.toString();
             String[] index_doc = str.split("\t");
             String[] doc_num = index_doc[1].split(",");

             for(String doc : doc_num)
             {
                 for(int i = 1 ; i<= 100 ; i++)
                 {
                     hash_output = ((Integer.valueOf(index_doc[0])*i+(100-i)) % prime)%N; // ((a*index+b)modP ) mod N
                     context.write(new Text(doc) , new Text(String.valueOf(i)+","+String.valueOf(hash_output)));
                 }
             }
        }
    }

    public static class Reducer3 extends Reducer<Text,Text,Text,Text> {

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

                String sig[] = new String[100];
                int index = 0;
                int hashvalue = 0;
                for(int i = 0 ; i< 100;i++) // simulate initial sig be infinity(9999)
                {
                    sig[i] = "99999";
                }
                for(Text value : values)
                {
                    String str = value.toString();
                    String[] index_sig = str.split(",");
                    index = Integer.valueOf(index_sig[0]);
                    hashvalue = Integer.valueOf(index_sig[1]);
                    if(Integer.valueOf(sig[index-1])>hashvalue) // if hash value smaller than sig value, then update sig.
                        sig[index-1] = String.valueOf(hashvalue);
                }
                String sigvalue = "";
                for(int i = 0 ; i < 100 ; i++)
                {
                    sigvalue = sigvalue+sig[i]+",";
                }
                context.write(key,new Text(sigvalue));
          }
      }

  public static class Mapper4 extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int r = 2;
            int band = 50;
            String str = value.toString();
            String[] doc_sig = str.split("\t");
            String doc = doc_sig[0];
            String[] sig = doc_sig[1].split(",");

            for(int i = 0 ; i<sig.length;i+=r)
            {
                String sig_r = "";
                for(int j = i ; j<i+r ; j++)
                {
                    sig_r = sig_r+sig[j]+",";

                }
                sig_r = sig_r +">"+String.valueOf(band);
                band--;
                context.write(new Text(sig_r),new Text(doc));

            }
        }
    }
    public static class Reducer4 extends Reducer<Text,Text,Text,Text> {

      private ArrayList<String> candidate = new ArrayList<String>();

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {


          ArrayList<String> docs = new ArrayList<String>();
          for (Text value : values) {
              docs.add(value.toString());
          }

          if(docs.size() > 1)
          {
              String pairs = "";
              String pairs_reverse = "";
              for(int i = 0 ; i < docs.size() - 1 ; i++)
              {
                  for(int j = i + 1 ; j < docs.size() ; j++)
                  {
                      pairs = docs.get(i) + "," + docs.get(j);
                      pairs_reverse = docs.get(j) + "," + docs.get(i);
                      if(!candidate.contains(pairs) && !candidate.contains(pairs_reverse))
                      {
                          candidate.add(pairs);
                          context.write(new Text("candidate pairs"), new Text(pairs));
                      }
                  }
              }
          }
       }

    }
    public static class Mapper5 extends Mapper<Object, Text, Text, Text>{
      private ArrayList<String> candidate = new ArrayList<String>();
      private ArrayList<String> sig_list = new ArrayList<String>();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           String newkey = "S";
           String str = value.toString();
           String[] pairs = str.split("\t");
           String newvalue = "";
           newvalue += pairs[0]+">"+pairs[1];
           context.write(new Text(newkey),new Text(newvalue));

        }
    }
    public static class Reducer5 extends Reducer<Text,Text,Text,Text> {

      private ArrayList<String> candidate = new ArrayList<String>();
      private ArrayList<String> sig_list = new ArrayList<String>();
      private ArrayList<String> result_sim = new ArrayList<String>();
      private ArrayList<String> result_pairs = new ArrayList<String>();

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
             Map<Double,String> map = new TreeMap<Double,String>(Collections.reverseOrder());
             double similarity = 0.0;
             String concat = "";
             String tmp_sim = "";
             String tmp_pairs = "";

             for(Text val : values)
             {
                 String str = val.toString();
                 String[] backterm = str.split(">");
                 String[] sig_or_can = backterm[1].split(",");
                 if(sig_or_can.length<3)   // Check  the format is canditate pairs or signature
                 {
                     for(String can : sig_or_can)
                         candidate.add(can);
                 }
                 else
                 {
                     for(String doc_sig :backterm )
                         sig_list.add(doc_sig);
                 }
             }

             for(int i = 0; i < candidate.size() ; i+=2)
             {
                 String pair_1 = candidate.get(i);
                 String pair_2 = candidate.get(i+1);
                 int pair1_index = sig_list.indexOf(pair_1);
                 int pair2_index = sig_list.indexOf(pair_2);
                 String sig_1 = sig_list.get(pair1_index+1);
                 String sig_2 = sig_list.get(pair2_index+1);
                 String[] compare_1 = sig_1.split(",");
                 String[] compare_2 = sig_2.split(",");
                 double same_num = 0.0;
                 for(int j = 0 ; j < 100 ;j++)   // count the intersection cardinality
                 {
                    if(compare_1[j].equals(compare_2[j]))
                        same_num++;
                 }
                 similarity = same_num/100;
                 String keypairs = "";
                 keypairs = "("+pair_1+","+pair_2+")";
                 result_pairs.add(keypairs);
                 result_sim.add(String.valueOf(similarity));

             }

             for(int i = 0 ; i < result_pairs.size()-1; i++) // Bubble sort to get decending order
             {
                 for(int j = 0; j < result_pairs.size()-i-1 ; j++)
                 {
                     if(Double.valueOf(result_sim.get(j))<Double.valueOf(result_sim.get(j+1)))
                     {
                         tmp_sim = result_sim.get(j);
                         tmp_pairs = result_pairs.get(j);
                         result_sim.set(j,result_sim.get(j+1));
                         result_sim.set(j+1,tmp_sim);
                         result_pairs.set(j,result_pairs.get(j+1));
                         result_pairs.set(j+1,tmp_pairs);
                     }
                 }

             }
             for(int i = 0 ; i < 10 ; i++)
             {
               String newkey = result_pairs.get(i);
               String newval = result_sim.get(i);
               context.write(new Text(newkey) , new Text(newval));

             }

          }



      }


public static void main(String[] args) throws Exception {
    /*Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "lsh");
    job1.setJarByClass(LSH.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    for(int i = 1 ; i <= 50 ; i++)
    {
        FileInputFormat.addInputPath(job1, new Path("data/" + String.format("%03d", i) + ".txt"));
    }
    FileOutputFormat.setOutputPath(job1, new Path("output/lsh_1"));
    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "lsh");
    job2.setJarByClass(LSH.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("output/lsh_1"));
    FileOutputFormat.setOutputPath(job2, new Path("output/lsh_2"));
    job2.waitForCompletion(true);

    Configuration conf3 = new Configuration();
    Job job3 = new Job(conf3, "lsh");
    job3.setJarByClass(LSH.class);
    job3.setMapperClass(Mapper3.class);
    job3.setReducerClass(Reducer3.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path("output/lsh_2"));
    FileOutputFormat.setOutputPath(job3, new Path("output/lsh_3"));
    job3.waitForCompletion(true);

    Configuration conf4 = new Configuration();
    Job job4 = new Job(conf4, "lsh");
    job4.setJarByClass(LSH.class);
    job4.setMapperClass(Mapper4.class);
    job4.setReducerClass(Reducer4.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path("output/lsh_3"));
    FileOutputFormat.setOutputPath(job4, new Path("output/lsh_4"));
    job4.waitForCompletion(true);*/

    Configuration conf5 = new Configuration();
    Job job5 = new Job(conf5, "lsh");
    job5.setJarByClass(LSH.class);
    job5.setMapperClass(Mapper5.class);
    job5.setReducerClass(Reducer5.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job5, new Path("output/lsh_4"));
    FileInputFormat.addInputPath(job5, new Path("output/lsh_3"));
    FileOutputFormat.setOutputPath(job5, new Path("output/lsh_5"));
    job5.waitForCompletion(true);


    }
}
