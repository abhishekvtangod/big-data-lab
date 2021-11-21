package average;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class reducer extends Reducer<Text,DoubleWritable,Text,Text> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        //     sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);
        double avg = 0;
        double cnt = 0;
        for(DoubleWritable val: values){
            cnt++;
            avg += val.get();
        }
        avg = avg/cnt;
        String out = "Total Count: " + cnt + " | Average Salary: " + avg; 
        context.write(key, new Text(out));
    }
}