package edu.cooper.ece460.picasa;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PicasaPartition extends Partitioner<Text, Writable>{
    @Override
        public int getPartition(Text key, Writable value, int numPartitions){
        return Math.abs(key.hashCode()) % numPartitions;
    }
}
