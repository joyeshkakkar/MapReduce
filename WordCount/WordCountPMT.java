package edu.neu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPMT {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		/*private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();*/

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			//Hashmap to store the count of words per line
			Map<String,Integer> map = new HashMap<String,Integer>();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String lwCaseWord,currentWord;
			while (tokenizer.hasMoreTokens()) {		
				currentWord=tokenizer.nextToken();
				lwCaseWord=currentWord.toLowerCase();
				//Considering only the words starting with m,n,o,p,q and their upper-case versions
				if(lwCaseWord.startsWith("m") || lwCaseWord.startsWith("n") || lwCaseWord.startsWith("o")
						|| lwCaseWord.startsWith("p") || lwCaseWord.startsWith("q")){
					if(map.containsKey(currentWord)){
						//Count is increased if the word is already present
						int count=map.get(currentWord)==null?1:map.get(currentWord)+1;
						map.put(currentWord, count);
					}else{
						//If the word is encountered first time, count is put as 1
						map.put(currentWord, 1);
					}
				}
			}
			//Contents of hashmap is emitted once per line
			Iterator<Entry<String, Integer>> it = map.entrySet().iterator();
			while(it.hasNext()){
				Entry<String,Integer> entry=it.next();
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue().intValue()));
			}
		}
	}  

	public static class CustomPartitioner extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String word = key.toString();
			char letter = word.toLowerCase().charAt(0);
			//109 is ASCII code for m, so words starting with m are assigned to reducer 0, n to reducer 1 and so on.
			return (int) letter - 109;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountPMT.class);
		job.setMapperClass(TokenizerMapper.class);
		//Commenting the setting up of combiner
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//Setting up the reducers for each of the 5 alphabets m,n,o,p,q and their upper-case versions
		job.setNumReduceTasks(5);
		job.setPartitionerClass(CustomPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}