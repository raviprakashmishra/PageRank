package com.ravi.hw2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankMapReducer extends Configured{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		static enum WordCounters {INPUT_WORD_COUNT}

		private Text key = new Text();
		private Text val = new Text();
		private Text title = new Text();
		private double rank = 1.0;
		private int degree = 0;
		private int startIndex = 0;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
				
			String line = value.toString();
			String[] Nodes = line.split("[ \t]");

			title.set(Nodes[0]);
			if (Nodes.length >= 2) {
				if (Nodes[1].length() < 10) { 
					// this case will occur when there are more than 1 iteration,
					// since emitted value from maps are in form of link,rank,degree
					// first part is link,second is rank and third is degree
					System.out.println("I got rank here");
					rank = Double.valueOf(Nodes[1]);
					startIndex = 3;
					degree = Nodes.length - (startIndex-1);

					emitPageRankLinks(output, reporter, Nodes);
				} else { 
					// this condition holds true for first iteration
					// when there are no ranks in input file 
					// hence default rank has been taken as 1
					rank = 1.0;
					startIndex = 2;
					degree = Nodes.length - (startIndex-1);

					emitPageRankLinks(output, reporter, Nodes);
				}
			} else {
				// this condition will cover the case when there are no outlinks
				// for a title
				rank = 1.0;
				degree = Nodes.length - 1;

				val.set("retain 0");
				output.collect(title, val);
			}

		}

		private void emitPageRankLinks(OutputCollector<Text, Text> output,
				Reporter reporter, String[] parts) throws IOException {
			final StringBuilder sb = new StringBuilder();
			if (parts.length >= startIndex) {
				for (int i = startIndex-1; i < parts.length; i++) {
					sb.append(parts[i] + " ");
				}
			}
			val.set("retain " + String.valueOf(degree) + " "
					+ sb.toString());
			output.collect(title, val);

			val.set(title.toString() + " "
					+ String.valueOf(rank) + " "
					+ String.valueOf(degree));
			if (parts.length >= startIndex) {
				for (int i = startIndex-1; i < parts.length; i++) {
					key.set(parts[i]);
					output.collect(key, val);
					reporter.incrCounter(WordCounters.INPUT_WORD_COUNT, 1);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private double lambda = 0.85;
		private Text val = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			double sum = 0.00;
			StringBuilder outlinks = new StringBuilder();
			String line = "";
			String[] parts ;
			//Double rank = 0.0d;
			//Double degree = 0.0d;

			while (values.hasNext()) {
				line = values.next().toString();
				parts = line.split("[ \t]");
				Double rank = Double.valueOf(parts[1]);

				if (!parts[0].equals("retain")) {
					// how much part of node is contributed to 
					// this outgoing edge
					Double degree = Double.valueOf(parts[2]);
					
					sum += rank / degree;
				} else {
					if (parts.length > 2) {
						for (int i = 2; i < parts.length; i++) {
							outlinks.append(parts[i] + " ");
						}
					}
				}
			}
			sum = lambda * sum + 1 - lambda;
			sum = Math.round(sum * 10000.0) / 10000.0;
			val.set(String.valueOf(sum) + " " + outlinks.toString());
			output.collect(key, val);
		}
	}

}
