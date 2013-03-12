package edu.cooper.ece460.picasa;

import java.io.*;
import java.net.URL;
import javax.imageio.ImageIO;
import java.util.LinkedList;
import java.awt.image.BufferedImage;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PicasaMap extends 
	Mapper<LongWritable, Text, Text, Text> 
{
	private int numImages, offset;
	private LinkedList<URL> imageURLs;
	private String searchTerm, unequalizedSaveLocation, equalizedSaveLocation;
	private Configuration conf;
	private FileSystem fs;
	private Context context;

	@Override
	public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException
	{
		this.context = context;

		String line = value.toString();
		String[] lineParts = line.split(",");
		String imageURL = lineParts[0];
		String unequalizedPath = lineParts[1];
		String equalizedPath = lineParts[2];

		conf = new Configuration();
		fs = FileSystem.get(conf);
		
		String filename = fetchImageAndSave(imageURL,unequalizedPath);
		unequalizedPath += "/" + filename;
		equalizedPath += "/" + filename;
        System.out.println(filename);
		context.write(
			new Text(unequalizedPath + "," + equalizedPath), // Replace this with a better key (partitioner?)
			new Text(unequalizedPath + "," + equalizedPath)
		);
	}

	private String getFilenameFromURL(String url) {
		int slashIndex = url.lastIndexOf('/');
		String filename = url.substring(slashIndex+1);
		return filename;
	}

	/* Go through the image URLs in the imageURLs linked list 
	    and save images to hdfs */
	private String fetchImageAndSave(String urlString, String saveLocation) 
		throws IOException 
	{
		BufferedImage img = ImageIO.read(new URL(urlString));
		String filename = getFilenameFromURL(urlString);
		Path outputPath = new Path(saveLocation + "/" + filename);

		ImageIO.write(img, "jpg", fs.create(outputPath));
		return filename;
	}
}

