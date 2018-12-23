package com.zhiyou.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class WordCountMapReduce extends Configured implements Tool{
    // step1:map class
    /**
     *
     * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     *
     */
    public static class WordCountMapper extends
           Mapper<LongWritable,Text,Text,IntWritable>{
       private Text mapOutputKey = new Text();
       private final static IntWritable mapOutputValue = new IntWritable(1);
       
       @Override
       public void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {
           // TODO
           String lineValue = value.toString();
           //split
           //String[] strs = lineValue.split(" ");
           StringTokenizer strs = new StringTokenizer(lineValue);
           
           //iterator
           while(strs.hasMoreTokens()) {
               //get word value
               String wordValue = strs.nextToken();
               //set value
               mapOutputKey.set(wordValue);
               //output
               context.write(mapOutputKey, mapOutputValue);
           }
           
       }
    }
    // step1:reduce class
    /**
     *
     * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     *
     */
    public static class WordCountReducer extends
           Reducer<Text,IntWritable, Text,IntWritable>{
       private IntWritable outputValue = new IntWritable();
       
       @Override
       protected void reduce(Text key, Iterable<IntWritable> values,
               Context context) throws IOException, InterruptedException {
           // TODO
           //sum tmp
           int sum = 0;
           //iterator
           for(IntWritable value: values) {
               sum += value.get();
           }
           
           //set value
           outputValue.set(sum);
           //output
           context.write(key, outputValue);
           
       }
    }
    // step1:driver job
    public int run(String[] args) throws Exception {
       //1 :get confifuration
       Configuration conf = getConf();
       
       //2:creat job
       Job job = Job.getInstance(conf,
               this.getClass().getSimpleName());
       //run jar
       job.setJarByClass(this.getClass());
       
       //3:set job
       //input -> map -> reduce -> output
       //3.1: input
       Path inPath = new Path(args[0]);
       FileInputFormat.addInputPath(job, inPath);
       
       //3.2: map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
       
       //3.3: reduce
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
       
       //3.4: output
       Path outPath = new Path(args[1]);
       FileOutputFormat.setOutputPath(job, outPath);
       
       //4 : submit job
       boolean isSuccess = job.waitForCompletion(true);
       
       return isSuccess ? 0 : 1;
    }
    
    
    /*public int run(String[] args) throws Exception {
       // TODO Auto-generated method stub
       return 0;
    }*/
    
    
    //step 4: run program
    public static void main(String[] args) throws Exception {
       
       //1 :get confifuration
       Configuration conf = new Configuration();
       
       //int status = new WordCountMapReduce().run(args);
       int status = ToolRunner.run(conf,
               new WordCountMapReduce(),
               args);
       //stop program
       System.exit(status);
    }
    
}