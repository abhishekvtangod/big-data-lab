package odd_or_even;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class mapper extends Mapper<Object, Text, Text, IntWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        //   StringTokenizer itr = new StringTokenizer(value.toString());
        //   while (itr.hasMoreTokens()) {
        //     word.set(itr.nextToken());
        //     context.write(word, one);
        //   }
        String data[] = value.toString().split(" ");
        for(String num: data){
            int n = Integer.parseInt(num);
            if(n%2 == 1){
                context.write(new Text("ODD"), new IntWritable(n));
            } else{
                context.write(new Text("EVEN"), new IntWritable(n));
            }
        }

    }
}