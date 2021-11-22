package matrix_mul;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class driver {
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // conf.set("m", 2);
    // conf.set("n", 5);
    // conf.set("p", 3);

    Job job = Job.getInstance(conf, "matrix multiplication");
    // job.setJarByClass(WordCount.class);
    job.setMapperClass(mapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
