package edu.cooper.ece460.picasa;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

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
	private FileSystem fs;
	private Context context;

	@Override
	public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException
	{
		this.context = context;

		// Get line with comma-delimited values
		String line = value.toString();
		String[] lineParts = line.split(",");

		int page = Integer.parseInt(lineParts[0]);
		numImages = Integer.parseInt(lineParts[1]);
		searchTerm = lineParts[2];
		unequalizedSaveLocation = lineParts[3];
		equalizedSaveLocation = lineParts[4];

		fs = FileSystem.get(new Configuration());

		offset = page*numImages+1;
		imageURLs = new LinkedList<URL>();

		fetchImageURLs();
		fetchImagesAndSave();
	}

	/* Fetches image URLs from Picasa API and pushes URL objects
	    to the imageURLs linked list */
	private void fetchImageURLs() {
		try {
			String urlString = "https://picasaweb.google.com/data/feed/api/all?";
			urlString += "q="+searchTerm;
			urlString += "&max-results="+numImages;
			urlString += "&start-index="+offset;

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

				// Push the image URLs on the queue to be used by fetchImagesAndSave()
				imageURLs.push(new URL(imageUrlString));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* Go through the image URLs in the imageURLs linked list 
	    and save images to hdfs */
	private void fetchImagesAndSave() {
		URL currentURL;
		BufferedImage img;
		String filename, urlString, equalizedPathString, unequalizedPathString;
		Path outputPath;
		int slashIndex;
		Text outputKey = new Text("" + offset);

		while (imageURLs.size() > 0) {
			try {
				currentURL = imageURLs.pop();
				img = ImageIO.read(currentURL);

				// Get file name
				urlString = currentURL.toString();
				slashIndex = urlString.lastIndexOf('/');
				filename = urlString.substring(slashIndex+1);

				// Save to hdfs
				unequalizedPathString = unequalizedSaveLocation + "/" + filename;
				equalizedPathString = equalizedSaveLocation + "/" + filename;

				outputPath = new Path(unequalizedPathString);
				ImageIO.write(img, "jpg", fs.create(outputPath));

				// Output 
				context.write(
					outputKey, 
					new Text(unequalizedPathString + "," + equalizedPathString)
				);
			}
			catch (Exception e) {
				// Couldn't process image. Move on to the next one.
				// TODO: log4j
				e.printStackTrace();
			}
		}
	}
}

