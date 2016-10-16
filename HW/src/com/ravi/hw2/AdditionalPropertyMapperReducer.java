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

public class AdditionalPropertyMapperReducer extends Configured {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		static enum Counters {
			INPUT_WORDS
		}

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private int degree = 0;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] split = line.split("[ \t]");

			degree = split.length - 1;
			outputKey.set("AddInfo");
			outputValue.set(String.valueOf(degree));
			output.collect(outputKey, outputValue);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private int edgesCount = 0;
		private int nodesCount = 0;
		private int minDeg = Integer.MAX_VALUE;
		private int maxDeg = Integer.MIN_VALUE;

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				String line = values.next().toString();
				String[] split = line.split("[ \t]");
				nodesCount++;

				int degree = Integer.parseInt(split[0]);
				edgesCount += degree;
				if (degree < minDeg)
					minDeg = degree;
				if (degree > maxDeg)
					maxDeg = degree;
			}
			if (key.toString().equals("AddInfo")) {
				Text out = new Text();
				out.set("\nNumber of Nodes: " + nodesCount
						+ "\nNumber of Edges: " + edgesCount
						+ "\nMinimum Degree: " + minDeg + "\nMaximum Degree: "
						+ maxDeg + "\nAverage Degree: " + edgesCount
						/ nodesCount);
				output.collect(key, out);
			} else {
				output.collect(key, values.next());
			}
		}
	}
}
