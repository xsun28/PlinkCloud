package org.plincloud.vcf.simulator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import io.netty.util.internal.ThreadLocalRandom;

public class VCFSimulator extends Configured implements Tool{
	private enum Quality {
		NULL,Q10,Q20,PASS
	};	
	enum TotalCounter{
		chr1, chr2, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chr10, chr11, chr12, chr13, chr14, chr15, chr16,
		chr17, chr18, chr19, chr20, chr21, chr22, chrx, chry, chrxy, chrm;
	}
	enum PassCounter{
		chr1, chr2, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chr10, chr11, chr12, chr13, chr14, chr15, chr16,
		chr17, chr18, chr19, chr20, chr21, chr22, chrx, chry, chrxy, chrm;
	}
	enum MaxCounter{
		chr1, chr2, chr3, chr4, chr5, chr6, chr7, chr8, chr9, chr10, chr11, chr12, chr13, chr14, chr15, chr16,
		chr17, chr18, chr19, chr20, chr21, chr22, chrx, chry, chrxy, chrm;
	}


	public static class StatisticsMapper extends Mapper<LongWritable,Text, NullWritable, NullWritable>{
		private int preChr = 0;
		private long preMax = 0;
		private Quality getQuality(String line){
			if(line.startsWith("#")) return null;
			if(line.contains("PASS")) return Quality.PASS;
			else if(line.contains("q20")) return Quality.Q10;
			else if(line.contains("q10")) return Quality.Q20;
			else return null;
			
		}
		
		private int chrToNum(String chr){
			int chrnum;
			switch (chr.toLowerCase()) {
			case "m":
				chrnum = 26; break;
			case "xy":
				chrnum = 25; break;
			case "x":
				chrnum = 23; break;
			case "y":
				chrnum = 24; break;
			default:
				chrnum = Integer.parseInt(chr);
				break;
			}
			return chrnum;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			Quality qual = getQuality(line);
			if(null == qual) return;
			String[] fields = line.split("\\s+");
			String chrnum = fields[0].substring(fields[0].indexOf("r")+1).trim();
			int chrm = chrToNum(chrnum);
			long pos = Long.parseLong(fields[1]);		
			context.getCounter(TotalCounter.values()[chrm-1]).increment(1);
			if(qual == Quality.PASS)
				context.getCounter(PassCounter.values()[chrm-1]).increment(1);
			if(chrm != preChr){
				if(preChr != 0){
					long current_max = context.getCounter(MaxCounter.values()[preChr-1]).getValue();
					if(current_max < preMax) 
						context.getCounter(MaxCounter.values()[preChr-1]).setValue(preMax);
				}				
				preChr = chrm;
				preMax = pos;
			}else if(pos > preMax) preMax = pos;
		}
		
		@Override
		protected void cleanup(Context context){
			System.out.println("current chr is "+preChr);
			long current_max = context.getCounter(MaxCounter.values()[preChr-1]).getValue();
			if(current_max < preMax) 
				context.getCounter(MaxCounter.values()[preChr-1]).setValue(preMax);	
		}
		
	}
	
	public static class SimulationMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
		private boolean start = true;
		private int chrNum = 0;
		private static final String Q20 = "q20";
		private static final String PASS = "PASS";
		private static final String HEADER = "#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	NY50135_MAXGT	NY50135_POLY";
		private static final String ID_REF_ALT_QUAL = ".	A	C	100";
		private static final String INFO_FORMAT_MAXGT_POLY = "DP=10	GT:GQ	0/1:99	0/1:107";
		private boolean sorted;
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			sorted = conf.getBoolean("sorted", true);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			if(start){
				context.write(NullWritable.get(), new Text(HEADER));
				start = false;
			}
			String[] params = value.toString().split(",");
			System.out.println("line"+chrNum+" is "+value.toString());
			long total_average = (long) Double.parseDouble(params[0]);
			long record_freq = (long) Double.parseDouble(params[1]);
			double pass_freq = (double) Double.parseDouble(params[2]);
		
			if(total_average == 0){
				return;
			}
			chrNum++;
			String chr = "chr"+chrNumToString(chrNum);
			StringBuilder result = new StringBuilder();
			ThreadLocalRandom random = ThreadLocalRandom.current();
			if(sorted){
				for(long i = 0, begin = 0,  end = record_freq; i<total_average; i++)
				{	
					result.setLength(0);
					long pos = random.nextLong(begin+1,end+1);
					begin += record_freq;
					end += record_freq;
					result.append(chr+"\t").append(pos+"\t").append(ID_REF_ALT_QUAL+"\t");
					String filter = random.nextDouble()*10000 < pass_freq*10000 ? PASS : Q20;	
					result.append(filter+"\t").append(INFO_FORMAT_MAXGT_POLY);				
					context.write(NullWritable.get(),new Text(result.toString()));
				}
			}else{
				ArrayList<Long> beginPosList = new ArrayList<>();
				for(long i = 0; i<total_average; i++ )
					beginPosList.add(new Long(i*record_freq+1));
				Collections.shuffle(beginPosList);
				for(int i = 0; i<total_average; i++)
				{	
					long begin = beginPosList.get(i);
					long end = begin+record_freq;
					result.setLength(0);
					long pos = random.nextLong(begin,end);
					result.append(chr+"\t").append(pos+"\t").append(ID_REF_ALT_QUAL+"\t");
					String filter = random.nextDouble()*10000 < pass_freq*10000 ? PASS : Q20;	
					result.append(filter+"\t").append(INFO_FORMAT_MAXGT_POLY);				
					context.write(NullWritable.get(),new Text(result.toString()));
				}
			}
		}
		
		private String chrNumToString(int chr){
			String chr_string;
			switch(chr){
			case 23: chr_string = "X"; break;
			case 24: chr_string = "Y"; break;
			case 25: chr_string = "XY"; break;
			case 26: chr_string = "M"; break;
			default: chr_string = String.valueOf(chr);
			}
			return chr_string;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception{
		int code;
		String input = args[0];
		String base_output = args[1];
		String output = base_output+"/VCF/";
		int ind_num = Integer.parseInt(args[2]);
		int simu_num = Integer.parseInt(args[3]);
		boolean sorted = Boolean.parseBoolean(args[4]);
		Configuration conf = getConf();
		Job statsJob = Job.getInstance(conf,"Stats");
		statsJob.setJarByClass(getClass());
		FileInputFormat.setInputPaths(statsJob, new Path(input));
		statsJob.setOutputFormatClass(NullOutputFormat.class);
		statsJob.setMapperClass(StatisticsMapper.class);
		statsJob.setNumReduceTasks(0);
		code = statsJob.waitForCompletion(true)?0:1;
		if(code !=0 ) return code;
		String stats_file = base_output+"/temp/statistics";
		FileSystem fs =  FileSystem.get(URI.create(stats_file),conf);
		
		StringBuilder statsLine = new StringBuilder();
		try(PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(new Path(stats_file)))))){
			for(int i = 0; i<simu_num; i++){
				for(int j = 0; j<26; j++){
					statsLine.setLength(0);
					long total = statsJob.getCounters().findCounter(TotalCounter.values()[j]).getValue();
					long max = statsJob.getCounters().findCounter(MaxCounter.values()[j]).getValue();
					long pass = statsJob.getCounters().findCounter(PassCounter.values()[j]).getValue();
					double total_average = (double)total/(double)ind_num;
					double recordFreq;
					if(total_average !=0){
						recordFreq = Math.ceil(((max+1) / total_average));
						double passFreq =  (double) pass/ (double) total;
						statsLine.append(total_average+",").append(recordFreq+",").append(passFreq);
					}
					else {
						statsLine.append("0,").append("0,").append("0");
						continue;
					}
					pw.println(statsLine.toString());
				}
			}
			pw.flush();
		}
		conf.setInt("mapreduce.input.lineinputformat.linespermap", 26);
		conf.setBoolean("sorted", sorted);
		Job sim_job = Job.getInstance(conf,"Simulation");
		sim_job.setJarByClass(getClass());
		FileInputFormat.setInputPaths(sim_job, new Path(stats_file));
		sim_job.setInputFormatClass(NLineInputFormat.class);
		FileOutputFormat.setOutputPath(sim_job, new Path(output));
		FileOutputFormat.setCompressOutput(sim_job, true);
		FileOutputFormat.setOutputCompressorClass(sim_job, BZip2Codec.class);
		sim_job.setOutputKeyClass(NullWritable.class);
		sim_job.setOutputValueClass(Text.class);
		sim_job.setMapperClass(SimulationMapper.class);
		sim_job.setNumReduceTasks(0);
		code = sim_job.waitForCompletion(true)?0:2;
		return code;
	}
	
	public static void main(String[] args) throws Exception{  //hadoop jar plinkcloud-vcf-simulator.jar org.plincloud.vcf.simulator.VCFSimulator /user/hadoop/plinkcloud/input/ /user/hadoop/simulation/ 3 100 true -libjars
		ToolRunner.run(new VCFSimulator(), args);	
	}

}
