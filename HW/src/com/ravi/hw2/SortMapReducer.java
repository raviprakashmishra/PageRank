package com.ravi.hw2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortMapReducer {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] split = line.split("[ \t]");
			outVal.set(split[0]);
			outKey.set(String.valueOf(100.0 - Double.valueOf(split[1])));
			output.collect(outKey, outVal);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private int outputCount = 0;
		Text rank = new Text();
		double rankKey = 0.0d;

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				rankKey = Math.round((100.0 - Double.valueOf(key
						.toString())) * 10000.0) / 10000.0;
				rank.clear();
				rank.set(String.valueOf(rankKey));
				if (outputCount < 10) { 
					output.collect(rank, values.next());
					outputCount++;
				} else {
					values.next();
				}

			}
		}
	}

}
