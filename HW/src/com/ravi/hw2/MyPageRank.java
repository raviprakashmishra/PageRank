package com.ravi.hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyPageRank implements Tool{
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyPageRank(), args);
		System.exit(res);
		
	}

	public int createJobs(String[] args) throws IOException {
		int numIter = (args.length == 3) ? Integer.parseInt(args[2]) : 16;
		// CountNodesJob(args);
		

		addIterativeRankJob(args, numIter);

		rankSorterJob(args, numIter);

		addPropertyJob(args);
		
		return 0;
	}

	private void addPropertyJob(String[] args) throws IOException {
		JobConf conf = new JobConf(AdditionalPropertyMapperReducer.class);
		conf.setJobName("AdditionalProperty");
		conf.setMapperClass(AdditionalPropertyMapperReducer.Map.class);
		conf.setReducerClass(AdditionalPropertyMapperReducer.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "additionalInfo"));
		JobClient.runJob(conf);
		
	}

	private void rankSorterJob(String[] args, int numIter) throws IOException {
		FileSystem fs;
		JobConf conf = new JobConf(SortMapReducer.class);
		conf.setJobName("SortRank");
		conf.setMapperClass(SortMapReducer.Map.class);
		conf.setReducerClass(SortMapReducer.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf,
				new Path("out" + String.valueOf(numIter)));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		fs = FileSystem.get(conf);
		fs.delete(new Path("out" + String.valueOf(numIter)), true);
	}

	/*
	 * private void CountNodesJob(String[] args) throws IOException { JobConf
	 * conf = new JobConf(PageRank.class);
	 * 
	 * conf = new JobConf(MyPageRank.class);
	 * 
	 * conf.setJobName("CountNodes"); conf.setMapperClass(CountNodes.Map.class);
	 * conf.setReducerClass(CountNodes.Reduce.class);
	 * conf.setOutputKeyClass(Text.class); conf.setOutputValueClass(Text.class);
	 * conf.setInputFormat(TextInputFormat.class);
	 * conf.setOutputFormat(TextOutputFormat.class);
	 * FileInputFormat.setInputPaths(conf, args[0]);
	 * FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	 * JobClient.runJob(conf);
	 * 
	 * }
	 */

	private void addIterativeRankJob(String[] args, int numIter)
			throws IOException {

		JobConf conf = new JobConf(MyPageRank.class);
		FileSystem fs;
		fs = FileSystem.get(conf);
		Path inpath = new Path(" ");
		Path outpath = new Path(" ");

		for (int i = 1; i <= numIter; i++) {
			inpath = (i == 1) ? (new Path(args[0])) : (new Path("out"
					+ String.valueOf(i - 1)));
			outpath = new Path("out" + String.valueOf(i));
			conf = new JobConf(MyPageRank.class);
			// conf.set("nodeCount", value);

			conf.setJobName("IterativePageRank");
			conf.setMapperClass(RankMapReducer.Map.class);
			conf.setReducerClass(RankMapReducer.Reduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, inpath);
			FileOutputFormat.setOutputPath(conf, outpath);
			JobClient.runJob(conf);
			fs = FileSystem.get(conf);
			if (i != 1) {
				fs.delete(new Path("out" + String.valueOf(i - 1)), true);
			}
		}
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		return new MyPageRank().createJobs(args);
	}
	
	

}
