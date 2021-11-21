package earthquake;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
// private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        // sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);
        double mxMag = Double.MIN_VALUE;
        for(DoubleWritable mag: values){
            mxMag = Math.max(mxMag, mag.get());
        }
        context.write(key, new DoubleWritable(mxMag));
    }
}