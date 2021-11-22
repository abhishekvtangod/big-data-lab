package matrix_mul;


import java.io.IOException;
import java.util.HashMap;
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


public class reducer extends Reducer<Text,Text,Text,Text> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        //     sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);
        String[] value;
        HashMap<Integer, Double> hashA = new HashMap<Integer, Double>();
        HashMap<Integer, Double> hashB = new HashMap<Integer, Double>();
        
        for(Text val: values){
            value = val.toString().split(",");
            if(value[0].equals("A")){
                hashA.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
            } else{
                hashB.put(Integer.parseInt(value[1]), Double.parseDouble(value[2]));
            }

        }
        int n = 5;
        double result = 0;
        double a_ij;
        double b_jk;
        for(int j = 0; j < n; j++){
            a_ij = hashA.containsKey(j) ? hashA.get(j) : 0;
            b_jk = hashA.containsKey(j) ? hashA.get(j) : 0;
            result += a_ij * b_jk;
        }
        if(result != 0){
            context.write(null, new Text(key.toString() + "," + Double.toString(result)));
        }

    }
}