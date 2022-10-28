import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Follower {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String BeFollow = tokenizer.nextToken();
            String Follow = tokenizer.nextToken();
            context.write(new IntWritable(Integer.parseInt(BeFollow)), new Text(Follow));
        }
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        //ArrayList<Text> Follow;
        String Follow = "";
        for (Text val : values) {
            Follow += (" " + val);
        }
        context.write(key, new Text(Follow));
    }
 }

 public static class FollowWhoMap extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String BeFollow = tokenizer.nextToken();
            String Follow = tokenizer.nextToken();
            context.write(new IntWritable(Integer.parseInt(Follow)), new Text(BeFollow));
        }
    }
 } 
        
 public static class FollowWhoReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        //ArrayList<Text> Follow;
        String FollowWho = "";
        for (Text val : values) {
            FollowWho += (" " + val);
        }
        context.write(key, new Text(FollowWho));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Follower");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Follower"));
        
    job.waitForCompletion(true);


    Job job2 = new Job(conf, "FollowWho");
    
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
        
    job2.setMapperClass(FollowWhoMap.class);
    job2.setReducerClass(FollowWhoReduce.class);
    job2.setJarByClass(WordCount.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/Follow_Who"));
        
    job2.waitForCompletion(true);
 }
        
}