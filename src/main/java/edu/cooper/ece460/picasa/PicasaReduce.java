package edu.cooper.ece460.picasa;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PicasaReduce extends 
	Reducer<Text, Text, Text, Text>
{
	private Context context;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws
		IOException, InterruptedException
	{
		this.context = context;
		
		for (Text value : values) {
			String[] lineParts = value.toString().split(",");
			String fileLocation = lineParts[0];
			String saveLocation = lineParts[1];

			context.write(
				new Text("read from: " + fileLocation), 
				new Text("save to: " + saveLocation)
			);
		}
	}
}


