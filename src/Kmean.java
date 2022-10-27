import java.io.IOException;
import java.util.*;

        
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
        
public class Kmean {

 public static ArrayList<ArrayList<Double>>  k = InitialK();

 public static ArrayList<ArrayList<Double>> InitialK(){
	
        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
	for(int i = 0; i < 3; i++){ //k=3
		ArrayList<Double> temp = new ArrayList<Double>();
		for(int j = 0; j < 24; j++){ // 24 dimansion
			temp.add((Double)(Math.random()*50));
		}
		result.add(temp);
	}
	
	return result;
 }

 
 public static ArrayList<ArrayList<Double>> getK(Path path) throws IOException, InterruptedException {
	
        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
	Configuration conf = new Configuration();
	

	FileSystem filesystem = path.getFileSystem(conf);

	FSDataInputStream fsis = filesystem.open(path);
	LineReader lineReader = new LineReader(fsis, conf);

	Text line = new Text();

	while(lineReader.readLine(line) > 0){
		String tempLine = line.toString();
		StringTokenizer tokenizer = new StringTokenizer(tempLine, ",");
		ArrayList<Double> tempList = new ArrayList<Double>();
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			tempList.add(Double.parseDouble(token));
		}
		result.add(tempList);
	}
	int size = result.size();
	for(int i = size; i < 3; i++){
		ArrayList<Double> temp = new ArrayList<Double>();
		for(int j = 0; j < 24; j++){
			temp.add((Double)(Math.random()*50));
		}
		result.add(temp);
	}
	return result;
 }
 

 public static boolean compareCenter(ArrayList<ArrayList<Double>> old, ArrayList<ArrayList<Double>> newone){
	int size = 24;
	
	double distance = 0;

	for(int i = 0; i < 3; i++){
		for(int j = 0; j < size; j++){
			double pm1 = Math.abs(old.get(i).get(j));
			double pm2 = Math.abs(newone.get(i).get(j));
			distance += Math.pow(pm1-pm2, 2);
		}
	}

	if(Math.round(distance) == 0){
		return true;
	}
	return false;
 }
 
 public static double distance(ArrayList<Double> a, ArrayList<Double> b){
	double result = 0.0;
	
	for(int i = 0; i < 24; i++){
		double pm1 = Math.abs(a.get(i));
		double pm2 = Math.abs(b.get(i));
		result += Math.pow(pm1-pm2, 2);
	}
	
	result = Math.sqrt(result);
	return result;
 }

 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

     

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	int count = 0;
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
	ArrayList<Double> temp = new ArrayList<Double>();
	double mindistance = 99999999;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
	    if(count > 2){
		if(token == ""){
			token = "0";
		}
		if(token.matches("[+-]?\\d*(\\.\\d+)?")){
			temp.add(Double.parseDouble(token));	
		}
	    }
	    count++;
        }
	int minID = 0;
	for(int i = 0; i < 3; i++){
		if(temp.size() != 24){
			break;
	  	}
		double tempD = distance(temp, k.get(i));
		if(mindistance > tempD){
			mindistance = tempD;
			minID = i;
		}
	}

        context.write(new IntWritable(minID), new Text(line));
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
	double[] avg = new double[24];
	ArrayList<ArrayList<Double>> Group = new ArrayList<ArrayList<Double>>();
        for (Text val : values) {
	    ArrayList<Double> temp = new ArrayList<Double>();
	    String line = val.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
	    while(tokenizer.hasMoreTokens()){
		String token = tokenizer.nextToken();
		if(token == ""){
			token = "0";
		}
		if(token.matches("[+-]?\\d*(\\.\\d+)?")){
			temp.add(Double.parseDouble(token));	
		}
            }
	    if(temp.size() == 24){
		Group.add(temp);
	    }
        }
	for(int i = 0; i < 24; i++){
		
        	double sum = 0;
		for(int j = 0; j < Group.size(); j++){
			sum+=Group.get(j).get(i);	
		}
		avg[i] = Math.round((sum/Group.size())*100)/100;
	}
	context.write(new Text(""), new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
    }
 }

 public static class ResultReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void Reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      for(Text val : values) {
	  context.write(key, val);
      }  

    }

 }
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
        
    int i = 0;
    while(i < 5){
	if(i >= 1){
        	ArrayList<ArrayList<Double>> newK = getK(new Path(args[1]+"/result"+(i-1)+"/center/part-r-00000"));
		if(compareCenter(k, newK)){
			break;
		}
		k = newK;
		//Path path = new Path(args[1]);
		//FileSystem hdfs = path.getFileSystem(conf);
		//hdfs.delete(path, true);
	}
    	Job job = new Job(conf, "Kmean"+i);
    
    	job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(Text.class);
        
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    	job.setJarByClass(WordCount.class);
        
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
        
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]+"/result"+i+"/center"));
        
    	job.waitForCompletion(true);
    
    	Job job2 = new Job(conf, "KmeanResult"+i);
	ArrayList<ArrayList<Double>> tempK = k;
    	k = getK(new Path(args[1]+"/result"+i+"/center/part-r-00000"));
    
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
        
    	job2.setMapperClass(Map.class);
    	job2.setReducerClass(ResultReduce.class);
    	job2.setJarByClass(WordCount.class);
        
    	job2.setInputFormatClass(TextInputFormat.class);
    	job2.setOutputFormatClass(TextOutputFormat.class);
        
    	FileInputFormat.addInputPath(job2, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/result"+i+"/class"));
    	job2.waitForCompletion(true);
	
	k=tempK;
	
	i++;
    }

        
 }
        
}
