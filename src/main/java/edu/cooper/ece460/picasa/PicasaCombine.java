package edu.cooper.ece460.picasa;

import java.io.*;
import java.util.*;
import java.net.*;
import java.awt.image.BufferedImage;
import java.awt.Color;
// import java.io.File;
import java.lang.Math;
import javax.imageio.ImageIO;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PicasaCombine extends 
                              Reducer<Text, Text, Text, Text>{
    private Context context;
    private FileSystem fs;
    private BufferedImage image;
    private String infile, outfile;

    @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws
        IOException, InterruptedException{
        this.context = context;
        fs = FileSystem.get(new Configuration());

        for (Text value : values) {
            System.out.println(value.toString());
            String[] lineParts = value.toString().split(",");
            infile = lineParts[0];
            outfile = lineParts[1];

            try{
                //image = ImageIO.read(new File(key.toString()));
                image = ImageIO.read(fs.open(new Path(infile)));
                equalize();
            }
            catch(Exception e){
                e.printStackTrace();
            }

            // context.write(
            //               new Text("read from: " + infile), 
            //               new Text("save to: " + outfile)
            //               );
            context.write(
                          value, 
                          value
                          );
        }
    }

    private void equalize(){
        int fileNum = 0;
        int height = image.getHeight();
        int width = image.getWidth();

        float[] hsb = new float[]{0,0,0,0};
        int[] pixels = image.getRGB(0, 0, width, height, null, 0, width);
        int[] histogram = new int[256];
        int cdfMin = 0;

        int i;
        Color c;

        // generate histogram of brightness values
        for(i = 0; i < pixels.length; i++){
            c = new Color(pixels[i]);
            hsb = Color.RGBtoHSB(c.getRed(), c.getGreen(), c.getBlue(), hsb);
            histogram[(int)(hsb[2]*255)]++;
        }

        // find first non-zero element of histogram
        for(i = 0; histogram[i] == 0; i++);

        cdfMin = histogram[i]; // because a cdf monotonically increases

        // convert histogram to cdf
        if(i == 0){
            i++;
        }

        for(; i < 256; i++){
            histogram[i] += histogram[i-1];
        }

        for(i = 1; i < 256; i++){
            histogram[i] = Math.round((((float) histogram[i])-cdfMin)/(height*width-cdfMin)*255);
        }

        for(i = 0; i < pixels.length; i++){
            c = new Color(pixels[i]);
            hsb = Color.RGBtoHSB(c.getRed(), c.getGreen(), c.getBlue(), hsb);
            hsb[2] = ((float) (histogram[(int)(hsb[2]*255)]))/255;
            pixels[i] = Color.HSBtoRGB(hsb[0], hsb[1], hsb[2]);
        }

        BufferedImage equalizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        equalizedImage.setRGB(0, 0, width, height, pixels, 0, width);

        // File outputfile1 = new File("unequalized " + outfile + ".jpg");
        File outputfile2 = new File("equalized " + outfile + ".jpg");
            
        try{
            //ImageIO.write(image, "jpg", outputfile1);
            //ImageIO.write(equalizedImage, "jpg", outputfile2);
            ImageIO.write(equalizedImage, "jpg", fs.create(new Path(outfile)));
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }





}


