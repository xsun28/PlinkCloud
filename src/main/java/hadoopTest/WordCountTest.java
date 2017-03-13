//package hadoopTest;
//
//
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Map.Entry;
//
//
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class WordCountTest extends Configured implements Tool{
//	Logger logger = LoggerFactory.getLogger(getClass());
//@Override
//	public int run(String [] args)throws Exception{
//	if(args.length != 2){
//		logger.info("Wrong number {} of arguments",args.length);
//		ToolRunner.printGenericCommandUsage(System.err);
//		return -1;
//	}
//	Job job = new Job(getConf(),"WordCount");
//	job.setJarByClass(getClass());
//	FileSystem system = FileSystem.get(getConf());
//	Path input = new Path(args[0]);
//	Path output = new Path(args[1]);
//	system.copyFromLocalFile(new Path("/home/cloudera/wordcount"), input);
//	FileInputFormat.addInputPath(job, input);
//	FileOutputFormat.setOutputPath(job, output);
//	job.setMapperClass(WordCountMapper.class);
//	job.setReducerClass(WordCountReducer.class);
//	job.setCombinerClass(WordCountReducer.class);
//	job.setOutputKeyClass(Text.class);
//	job.setOutputValueClass(IntWritable.class);
//	
//	return job.waitForCompletion(true)?0:1;
//	}
//
//public static class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
//	@Override
//	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
//		Map<String, Integer> parsed = parseText(value.toString());
//	for(Entry<String,Integer> item:parsed.entrySet()){
//		context.write(new Text(item.getKey()), new IntWritable(item.getValue()));
//	}
//	}
//	
//	private Map<String, Integer>parseText(String text){
//		String[] words = text.split("\\s");
//		Map<String, Integer> map = new HashMap();
//		for(String word:words){
//			if(map.containsKey(word.trim())) 
//				map.put(word.trim(), new Integer(map.get(word.trim()).intValue()+1));
//			else
//				map.put(word.trim(), 1);
//		}
//		return map;
//	}
//}
//
//public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
//	@Override
//	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
//		int sum = 0;
//		for(IntWritable value: values)
//			sum+=value.get();
//		context.write(new Text(key), new IntWritable(sum));
//	}
//}
//
//@Test
//public void test() throws IOException, InterruptedException{
//	Text value1 = new Text(" day");
//	Text value2 = new Text("day");
//	new MapDriver<LongWritable,Text,Text,IntWritable>()
//	.withMapper(new WordCountMapper())
//	.withInput(new LongWritable(0), value1)
//	.withInput(new LongWritable(1),value2)
//	.withOutput(new Text("day"),new IntWritable(2))
//	.runTest();
//}
//
//public static void main(String[] args)throws Exception{
//	int exitCode = ToolRunner.run(new WordCountTest(), args);
//	System.exit(exitCode);
//}
//	
//}
