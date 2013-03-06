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

public class PicasaEqualize {
    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Might want to add error checking to make sure arguments exist
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        Path mapInputPath = new Path(args[0] + "/map_input.csv");
        int numAPICalls = Integer.parseInt(args[2]);
        int numImagesPerCall = Integer.parseInt(args[3]);
        String searchTerm = args[4];

        // Write the csv input file to hdfs
        FSDataOutputStream fos = fs.create(mapInputPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

        for (int i=0; i<numAPICalls; i++) {
            writer.write(
                i + "," + 
                numImagesPerCall + "," + 
                searchTerm + "," + 
                args[0] + "," +
                args[1] + "\n"
            );
        }
        writer.close();
        fos.close();

        // Start job
        Job job = new Job(conf, "PicasaEqualize");
        job.setJarByClass(PicasaEqualize.class);
        job.setMapperClass(PicasaMap.class);
        // job.setCombinerClass(PicasaCombine.class);
        job.setReducerClass(PicasaReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, mapInputPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }
}

