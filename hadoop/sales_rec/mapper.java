package sales;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.PatternSyntaxException;

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

public class mapper extends Mapper<Object, Text, Text, IntWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
        // StringTokenizer itr = new StringTokenizer(value.toString());
        // while (itr.hasMoreTokens()) {
        //     word.set(itr.nextToken());
        //     context.write(word, one);
        // }
        String[] line = value.toString().split(",");
        if(line[0].equals("Transaction_date")){
            return; //header of csv
        }
        // for(String val: line){
        //     System.out.print(val + " | ");
        // }
        // System.out.println();

        String country = "_country_" + line[7];
        String payment_type = "_payment_type_" + line[3];
        int price = Integer.parseInt(line[2]);
        context.write(new Text(country), new IntWritable(price));
        context.write(new Text(payment_type), new IntWritable(1));
    }
}