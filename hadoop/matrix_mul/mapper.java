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

public class mapper extends Mapper<Object, Text, Text, Text>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
        // StringTokenizer itr = new StringTokenizer(value.toString());
        // while (itr.hasMoreTokens()) {
        //     word.set(itr.nextToken());
        //     context.write(word, one);
        // }
        // Configuration conf = context.getConfiguration();
        // int m = Integer.parseInt(conf.get("m"));
        // int n = Integer.parseInt(conf.get("n"));
        // int p = Integer.parseInt(conf.get("p"));
        int m = 2, n = 5, p = 2;
        String[] s = value.toString().split(",");
        
        // for(String val: s)
        //     System.out.print(val+",");
        // System.out.println();
        
        String outputKey = "";
        String outputValue = "";
        
        if(s[0].equals("A")){
            for(int k = 0; k < p; k++){
                outputKey = s[1] + "," + k;
                outputValue = "A," + s[2] + "," + s[3];
                context.write(new Text(outputKey), new Text(outputValue));
            }
        } else{
            for(int i = 0; i < m; i++){
                outputKey = i + "," + s[2];
                outputValue = "B," + s[1] + "," + s[3];
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }



    }
}