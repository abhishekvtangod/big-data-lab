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


public class mapper extends Mapper<Object, Text, Text, DoubleWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {

        // StringTokenizer itr = new StringTokenizer(value.toString());
        // while (itr.hasMoreTokens()) {
        //     word.set(itr.nextToken());
        //     context.write(word, one);
        // }
        String[] s = value.toString().split("\t");
        String sex = s[3];
        Double salary = Double.parseDouble(s[8]);
        context.write(new Text(sex), new DoubleWritable(salary));
    }
}