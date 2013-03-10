package edu.cooper.ece460.picasa;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

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

public class PicasaEqualize {
	public LinkedList<String> fetchImageURLs(String searchTerm, int numImages) 
		throws Exception
	{
		LinkedList<String> imageURLs = new LinkedList<String>();
		try {
			String urlString = "https://picasaweb.google.com/data/feed/api/";
			if (searchTerm == "") 
				urlString += "featured?";
			else 
				urlString += "all?q=" + searchTerm;


			urlString += "&kind=photo&access=public";
			urlString += "&max-results="+numImages;

			// Get XML from Picasa API
			URL url = new URL(urlString);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(url.openStream());
			doc.getDocumentElement().normalize();

			// Parse XML to obtain image URLs
			NodeList nList = doc.getElementsByTagName("content");
			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);
				Element eElement = (Element) nNode;
				String imageUrlString = eElement.getAttribute("src");

				imageURLs.push(imageUrlString);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return imageURLs;
	}

	private String getFilenameFromURL(String url) {
		int slashIndex = url.lastIndexOf('/');
		String filename = url.substring(slashIndex+1);
		return filename;
	}

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
		PicasaEqualize pe = new PicasaEqualize();

        // Might want to add error checking to make sure arguments are valid
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        int numImages = Integer.parseInt(args[2]);

		// If no search term passed in, search featured images
		String searchTerm = "";
		if (args.length == 4)
			searchTerm = args[3];

		// Save image URLs to text files in the input directory
		LinkedList<String> imageURLs = pe.fetchImageURLs(searchTerm, numImages);

		for (String imageURL : imageURLs) {
			// Save txt file containing image url to HDFS
			String filename = pe.getFilenameFromURL(imageURL);
			FSDataOutputStream fos = fs.create(new Path(inPath + "/" + filename + ".txt"));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));
			writer.write(imageURL + "," + args[0] + "," + args[1] + "\n");
			writer.close();
			fos.close();
		}

        // Start job
        Job job = new Job(conf, "PicasaEqualize");
        job.setJarByClass(PicasaEqualize.class);
        job.setMapperClass(PicasaMap.class);
        job.setReducerClass(PicasaReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }
}

