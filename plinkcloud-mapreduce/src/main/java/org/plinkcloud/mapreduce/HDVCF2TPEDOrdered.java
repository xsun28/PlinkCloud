/*using interval sampler for already sorted original files and transform directly into TPED format*/
package org.plinkcloud.mapreduce;
 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDVCF2TPEDOrdered extends Configured implements Tool {
	
	public static class ChrBinningMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>	{//filtering out unwanted fields and bin records into chromosome folders under VoTECloud/output/chr1
		
		private String filename;
		private MultipleOutputs<LongWritable, Text> mos;
		private long kept = 1;    											//number of kept records for sampling
		private long records = 0; 											//number of records currently read in the input split
		private double freq;		
		private enum Quality {
					NULL,Q10,Q20,PASS
		};		
		private Quality qual_filter;
		
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{ 		//called once at the beginning of a mapper with a single input split 
			
			Configuration conf = context.getConfiguration();
			freq = Double.parseDouble(conf.get("samplerate"));
			qual_filter = Enum.valueOf(Quality.class, conf.get("quality").trim().toUpperCase());
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fullname=fileSplit.getPath().getName();									//get the name of the file where the input split is from
			filename=fullname.substring(0, fullname.indexOf('.')); 							//the individual number			
			mos=new MultipleOutputs(context);
			
		}//end of setup
		
		private Quality getQuality(String line){
			if(line.startsWith("#")) return null; // header
			if(line.contains("PASS")) return Quality.PASS;
			else if(line.contains("q20")) return Quality.Q10;
			else if(line.contains("q10")) return Quality.Q20;
			else return Quality.NULL;
			
		}
		
		private String parseGenotype(String line){
			
			StringBuilder genotype = new StringBuilder();
			String numbered_genotype = null;//1/0, 1/1...
			Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
			String [] fields = line.split("\\s");
			String genotype_field = fields[9].trim();
			String [] alts = fields[4].trim().split(",");
			String ref = fields[3].trim();
			Matcher matcher = genotypePattern.matcher(genotype_field);
			if(matcher.find())
				numbered_genotype = genotype_field.substring(matcher.start(),matcher.end());
			String [] genotype_numbers = numbered_genotype.split("[\\/\\|]");
			for (int i=0;i<genotype_numbers.length;i++){
				int number = Integer.parseInt(genotype_numbers[i].trim());
				if(number==0)
					genotype.append(ref).append(" ");
				else
					genotype.append(alts[number-1]).append(" ");	
			}
			return genotype.toString().trim();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int chrm;
			LongWritable outkey = new LongWritable();
			StringBuilder outputResult = new StringBuilder();	
			String line = value.toString();
			Quality qual = getQuality(line);
			if(null != qual && qual.compareTo(qual_filter) >= 0){
			records++;
			String[] fields = line.split("\\s+");
			String outputFile;
			String chrnum = fields[0].substring(fields[0].indexOf("r")+1).trim();
			if(chrnum.equalsIgnoreCase("X")) chrm = 23;
			else if (chrnum.equalsIgnoreCase("Y")) chrm = 24;
			else if (chrnum.equalsIgnoreCase("M")) chrm = 25;
			else chrm = Integer.parseInt(chrnum);			
			outputFile = fields[0].trim()+"/part";				//output file for this record  
			String genotype = parseGenotype(line);
			outkey.set(Long.parseLong(fields[1]));
            if ((double) kept / (double)records < freq) {
			kept++;
			context.write(outkey, new IntWritable(chrm));
				}
			outputResult.append(filename+",").append(chrnum+",").append(fields[2].trim()+",").append(fields[3]+",").append(genotype);
			mos.write("ChrBinningMos",outkey, new Text(outputResult.toString()), outputFile);
			}
		}// end of map
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	
	}// end of chrBinningMapper

	public static class PartitionlstPartitioner extends Partitioner <LongWritable, IntWritable>{
		@Override
		public int getPartition(LongWritable key, IntWritable value, int numPartitions){
			int chrmnum = value.get();
			return chrmnum-1;
		}
	}

	public static class PartitionReducer extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {
		
		private int StepSize;
		private int samplenums = 0;
		private int last = 0;
		private int totalnums;
		private String chrm = null;
		private ArrayList<Long> samples = new ArrayList<Long> ();
		private BufferedWriter writer;
		private String sampleNumberFiles; 		//The file containing the number of sampled position
		private Configuration conf;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			conf = context.getConfiguration();
			StepSize = Integer.parseInt(conf.get("fileno"));
			int chrmnum = context.getTaskAttemptID().getTaskID().getId()+1;
			if(chrmnum == 23) chrm = "X";
			else if(chrmnum == 24) chrm = "Y";
			else if(chrmnum == 25) chrm = "M";
			else chrm =""+chrmnum;
			sampleNumberFiles = conf.get("outputpath")+"/chr"+chrm+"samplenum";
		}

		@Override
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//outkey = key;
			long longkey = key.get();
			for(IntWritable value: values){
				samplenums++;
				if(samplenums>=StepSize){ // take one sample every file_no
					if(last!=0){
						long lastkey = samples.get(last-1);
						if(longkey != lastkey) {
							samples.add(longkey);
							last++;
							context.write(key,NullWritable.get());
							samplenums = 0;
						}
						else {
							samplenums--;
							break;
						}	
					}else {
						samples.add(longkey);
						last++;
						context.write(key,NullWritable.get());
						samplenums = 0;
					}
				}
			}
		}//end of reduce
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			totalnums = samples.size();
			FileSystem fs = FileSystem.get(URI.create(sampleNumberFiles), conf);
	    	try{
	    		writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(sampleNumberFiles), true)));
	    		System.out.println("total sample nums in "+chrm+" is "+totalnums);
	    		writer.write(""+totalnums);
	    	}finally{
	    	writer.close();
	    	}	
		}//end of cleanup
		
	}//end of partitionReducer

	public static class ValueReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		private int total;
		private MultipleOutputs<NullWritable, Text> mos;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration con = context.getConfiguration();
			total=Integer.parseInt(con.get("fileno"));
			mos=new MultipleOutputs(context);
		}
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder outputStr = new StringBuilder();
			boolean [] notNull = new boolean[total];
			String ref = null;
			String [] genotypes=new String[total];
			String chrm = null;
			boolean first = true;
			for (Text t : values) {
				String line = t.toString();
				String[] temp = line.split(",");
				if(first){ 
					chrm = "chr"+temp[1];
					ref = temp[3].trim();
					outputStr.append(temp[1]+"\t").append(temp[2]).append("\t0\t").append(key.toString()+"\t");
					first = false;
				}
				int current = Integer.parseInt(temp[0]);
				genotypes[current-1] = temp[4];
				notNull[current-1] = true;
			}
	
			for(int count=1;count<=total;count++){
				if(!notNull[count-1]){
					outputStr.append(ref+" ").append(ref+" ");
				}
				else{				
					outputStr.append(genotypes[count-1]+" ");
				}
			}
			mos.write(NullWritable.get(),new Text(outputStr.toString()), chrm+".tped/part");

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}


	public static Configuration getSecondJobConf( Tool tool, Configuration conf, String chrm, Path inputPath, Path outputPath, String basePath, double samplerate, int indno, boolean compression)
		 throws IOException, ClassNotFoundException, InterruptedException{
	
		Job orderJob = new Job(conf);
		orderJob.setJobName(chrm+" job");
		orderJob.setJarByClass(tool.getClass());
		orderJob.setReducerClass(ValueReducer.class);
		orderJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
		orderJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
		FileInputFormat.setInputPaths(orderJob,  inputPath);
		orderJob.setInputFormatClass(SequenceFileReadCombiner.class);
		FileOutputFormat.setOutputPath(orderJob, outputPath);
		if(compression){
			FileOutputFormat.setCompressOutput(orderJob,true);
			FileOutputFormat.setOutputCompressorClass(orderJob, BZip2Codec.class);
		}
		orderJob.setOutputKeyClass(LongWritable.class);
		orderJob.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(orderJob, TextOutputFormat.class);
		//orderJob.getConfiguration().set( "mapred.textoutputformat.separator", "");
	
		String file = basePath+"/part-r-000";  //path of the partition list files eg. VoTECloud/output/part-r-00022
		if(chrm.equalsIgnoreCase("X")) file += "22";
		else if(chrm.equalsIgnoreCase("Y")) file += "23";
		else if(chrm.equalsIgnoreCase("M")) file += "24";
		else if(chrm.equals("10")) file += "09";
		else if(chrm.length()==2) file += (Integer.parseInt(chrm)-1);
		else file += "0"+(Integer.parseInt(chrm)-1);
		Path partitionFile = new Path(file);
		String samplenumFileStr = basePath + "/chr"+chrm+"samplenum";
		Path samplenumFile = new Path(samplenumFileStr);
		FileSystem fs = FileSystem.get(URI.create(samplenumFileStr),conf);
		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(samplenumFile)));
		String samplenum = reader.readLine();
		System.out.println("retrieved samplenum for "+chrm+" is "+samplenum);
		int reducernum = Integer.parseInt(samplenum.trim())+1; //reducer num = split point number +1 
		reader.close();	
		orderJob.setPartitionerClass(TotalOrderPartitioner.class);
		if(fs.exists(partitionFile)){
			
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);// set the partition file to the output file from partitionreducer
			orderJob.setNumReduceTasks(reducernum);
		
		}else{
			
			Path regenerated_partitionFile = new Path(outputPath + "_partitions.lst"); //regenerate the partition list using the TotalOrderPartitioner for sparse chromsome which doesn't have a sampled-out list
			if(!fs.exists(regenerated_partitionFile)){ 
				int num = 500;
				System.out.println("partition file for "+chrm+" not existed");
				orderJob.setNumReduceTasks((indno/93)*4);
				orderJob.setInputFormatClass(SequenceFileReadCombinerSmall.class);
				TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), regenerated_partitionFile);
				InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler<LongWritable, Text>(samplerate*1000, num, 1));			
		}else{// if there is already re-generated partition file from previous run, use it
				orderJob.setNumReduceTasks((indno/93)*4);
				TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), regenerated_partitionFile);
			}
		}
		return orderJob.getConfiguration();
	}

	public int run(String [] args) throws Exception {
		
		int code = 0;
		Path inputPath = new Path(args[0]); // 		VoTECloud/input
		Path outputPath = new Path(args[1]); // 	VoTECloud/output
		int indno = Integer.parseInt(args[2].trim());
		double sampleRate = Double.parseDouble(args[3]); //0.0001
		String chrmrange = args[4];
		boolean compression = Boolean.parseBoolean(args[5]);
		String quality = args[6].trim();
		int index = chrmrange.indexOf("-");
		String firstchrm = chrmrange.substring(0, index).trim();
		String lastchrm = chrmrange.substring(index+1).trim();
		int first, last;
		if(firstchrm.equalsIgnoreCase("X"))
			first = 23;
		else if(firstchrm.equalsIgnoreCase("Y"))
			first = 24;
		else if(firstchrm.equalsIgnoreCase("M"))
			first = 25;
		else first = Integer.parseInt(firstchrm);

		if(lastchrm.equalsIgnoreCase("X"))
			last = 23;
		else if(lastchrm.equalsIgnoreCase("Y"))
			last = 24;
		else if(lastchrm.equalsIgnoreCase("M"))
			last = 25;
		else last = Integer.parseInt(lastchrm);

		int chromsno = last-first+1;
		Configuration conf = new Configuration();
		String defaultName = conf.get("fs.default.name");
		System.out.println("default "+defaultName);
		String []chroms = new String[chromsno];
		String []chrmFiles = new String[chromsno];
		String []chrmResults = new String[chromsno];
		for(int i=0;i<chromsno;i++, first++){
			if(first == 23)chroms[i] = "chrX";
			else if(first==24) chroms[i] = "chrY";
			else if(first==25) chroms[i] = "chrM";
			else chroms[i] = "chr"+first;
			chrmFiles[i] = defaultName+args[1]+"/"+chroms[i];
			chrmResults[i] = chrmFiles[i]+"_result";
		} 
		FileSystem fs = FileSystem.get(URI.create(chrmFiles[0]),conf);
		boolean exist = fs.exists(new Path(chrmFiles[0])); //test if chromosomes have been already binned
		conf.set("fileno", args[2]);
		conf.set("outputpath",defaultName+args[1]);
		conf.set("samplerate", ""+sampleRate);
		conf.set("quality", quality);
		if(!exist){
			Job chrJob = new Job(conf);
			chrJob.setJobName("chromseparation");
			chrJob.setJarByClass(getClass());
			FileInputFormat.setInputPaths(chrJob, inputPath);
			FileOutputFormat.setOutputPath(chrJob, outputPath);
			chrJob.setMapperClass(ChrBinningMapper.class);
			chrJob.setMapOutputKeyClass(LongWritable.class);
			chrJob.setMapOutputValueClass(IntWritable.class);
			chrJob.setPartitionerClass(PartitionlstPartitioner.class);
			chrJob.setReducerClass(PartitionReducer.class );
			chrJob.setOutputKeyClass(LongWritable.class);
			chrJob.setOutputValueClass(NullWritable.class);
			chrJob.setNumReduceTasks(25);
			chrJob.getConfiguration().setBoolean("mapred.compress.map.output", true);
			chrJob.getConfiguration().setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
			MultipleOutputs.addNamedOutput(chrJob, "ChrBinningMos",SequenceFileOutputFormat.class, LongWritable.class, Text.class);//multiple outputs for binning chromosomes				
			SequenceFileOutputFormat.setCompressOutput(chrJob, true);
			SequenceFileOutputFormat.setOutputCompressorClass(chrJob, Lz4Codec.class);
			SequenceFileOutputFormat.setOutputCompressionType(chrJob, CompressionType.BLOCK);
			LazyOutputFormat.setOutputFormatClass(chrJob, SequenceFileOutputFormat.class);
			code=chrJob.waitForCompletion(true)?0:1;
		}

		if(code==0){
			int leftchrmno = chromsno;
			boolean [] exited = new boolean[leftchrmno];
			JobControl jc = new JobControl("paralleljobs");
			ControlledJob []jobs;
			for(int i=0; i<chromsno;i++){
				boolean existedResult = fs.exists(new Path(chrmResults[i]));
				if(existedResult){
					leftchrmno--;
					exited[i] = true;	
				}
			}
			
			if(leftchrmno==0) {
				System.err.println("all files existed");
				return 1;
			}else{
				jobs = new ControlledJob[leftchrmno];
				for(int i=0; i<chromsno; i++){
					if(exited[i]) continue;
					jobs[i] = new ControlledJob(getSecondJobConf(this, conf,chroms[i].substring(3),new Path(chrmFiles[i]),new Path(chrmResults[i]),defaultName+args[1], sampleRate, indno, compression ));
					jc.addJob(jobs[i]);
				}
				JobRunner runner = new JobRunner(jc);
				Thread t = new Thread(runner);
				t.start();
				int i=1;
				while(!jc.allFinished()){
					System.out.println(i*5+"seconds elapsed...");
					i++;
					Thread.sleep(5000);
				}
				return 0;
			}
		}
		else return 2;

	}// end of run

	public static void main(String[] args) throws Exception{  //hadoop jar plinkcloud-mapreduce.jar org.plinkcloud.mapreduce.HDVCF2TPEDOrdered /user/cloudera/VoTECloud/input/ /user/cloudera/VoTECloud/output/ 3 0.0001 1-2 false PASS -libjars lib/..jar,..jar
		int code=ToolRunner.run(new HDVCF2TPEDOrdered(), args);
		System.exit(code);
	}

}