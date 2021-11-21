package insurance;

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


public class reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        // sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);
        int freq = 0;
        for(IntWritable val: values){
            // freq++;
            freq += val.get();
        }
        context.write(key, new IntWritable(freq));

    }
}
