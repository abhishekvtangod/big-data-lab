package sales;


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


public class reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        //     sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);
        String temp = key.toString();
        if(temp.substring(0, 9) == "_country_"){
            int total_sales = 0;
            for(IntWritable val: values){
                total_sales++;
            }
            context.write(key, new IntWritable(total_sales));
        } else{
            int payment_freq = 0;
            for(IntWritable val: values){
                payment_freq++;
            }
            context.write(key, new IntWritable(payment_freq));
        }
        // int cnt = 0;
        // for(IntWritable val: values){
        //     cnt++;
        // }
        // context.write(key, new IntWritable(cnt));
    }
}