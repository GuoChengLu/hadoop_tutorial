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

 public static ArrayList<ArrayList<Integer>>  k = InitialK();

 public static ArrayList<ArrayList<Integer>> InitialK(){
	
        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
	for(int i = 0; i < 3; i++){ //k=3
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int j = 0; j < 24; j++){ // 24 dimansion
			temp.add((int)(Math.random()*100));
		}
		result.add(temp);
	}
	
	return result;
 }

 
 public static ArrayList<ArrayList<Integer>> getK(Path path) throws IOException, InterruptedException {
	
        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
	Configuration conf = new Configuration();
	

	FileSystem filesystem = path.getFileSystem(conf);

	FSDataInputStream fsis = filesystem.open(path);
	LineReader lineReader = new LineReader(fsis, conf);

	Text line = new Text();

	while(lineReader.readLine(line) > 0){
		String tempLine = line.toString();
		StringTokenizer tokenizer = new StringTokenizer(tempLine, ",");
		ArrayList<Integer> tempList = new ArrayList<Integer>();
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			tempList.add(Integer.parseInt(token.trim()));
			//System.out.println(token);
		}
		result.add(tempList);
	}
	int size = result.size();
	for(int i = size; i < 3; i++){
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int j = 0; j < 24; j++){
			temp.add((int)(Math.random()*100));
		}
		result.add(temp);
	}
	return result;
 }
 

 public static boolean compareCenter(ArrayList<ArrayList<Integer>> old, ArrayList<ArrayList<Integer>> newone){
	int size = 24;
	
	int distance = 0;

	for(int i = 0; i < 3; i++){
		for(int j = 0; j < size; j++){
			int pm1 = Math.abs(old.get(i).get(j));
			int pm2 = Math.abs(newone.get(i).get(j));
			distance += Math.pow(pm1-pm2, 2);
		}
	}

	if(distance == 0){
		return true;
	}
	return false;
 }
 
 public static int distance(ArrayList<Integer> a, ArrayList<Integer> b){
	int result = 0;
	
	for(int i = 0; i < 24; i++){
		int pm1 = a.get(i);
		int pm2 = b.get(i);
		result += Math.pow(pm1-pm2, 2);
	}
	
	result = (int)Math.sqrt(result);
	return result;
 }

 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

     

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	int count = 0;
        String line = value.toString();
	String [] tokens = line.split(",");
	ArrayList<Integer> temp = new ArrayList<Integer>();
	int mindistance = 99999999;
	for(int i = 0; i < tokens.length; i++){

	    if(i > 2){
		if(tokens[i].isEmpty()){
			tokens[i] = "0";
		}
		tokens[i] = tokens[i].replace("x", "");
		if(tokens[i].matches("[+-]?\\d*(\\.\\d+)?")){
			temp.add(Integer.parseInt(tokens[i].trim()));	
            	}
	    }
	}
	int minID = 0;
	for(int i = 0; i < 3; i++){
		if(temp.size() != 24){
			break;
	  	}
		int tempD = distance(temp, k.get(i));
		if(mindistance > tempD){
			mindistance = tempD;
			minID = i;
		}
	}
	if(temp.size() == 24){
        	context.write(new IntWritable(minID), new Text(line));
	}
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
	int[] avg = new int[24];
	ArrayList<ArrayList<Integer>> Group = new ArrayList<ArrayList<Integer>>();
        for (Text val : values) {
	    ArrayList<Integer> temp = new ArrayList<Integer>();
	    String line = val.toString();
	    String [] tokens = line.split(",");
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
	    for(int i = 0; i < tokens.length; i++){

	        if(i > 2){
		    if(tokens[i].isEmpty()){
			tokens[i] = "0";
		    }
		    tokens[i] = tokens[i].replace("x", "");
		    if(tokens[i].matches("[+-]?\\d*(\\.\\d+)?")){
			temp.add(Integer.parseInt(tokens[i].trim()));	
            	    }
	        }
	    }
	    if(temp.size() == 24){
		Group.add(temp);
	    }
        }
	for(int i = 0; i < 24; i++){
		
        	int sum = 0;
		int size = Group.size();
		for(int j = 0; j < size; j++){
			sum+=Group.get(j).get(i);	
		}
		avg[i] = (int)(sum/size);
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
    while(i < 15){
	if(i >= 1){
        	ArrayList<ArrayList<Integer>> newK = getK(new Path(args[1]+"/result"+(i-1)+"/center/part-r-00000"));
		if(k == newK){
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
	ArrayList<ArrayList<Integer>> tempK = k;
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
