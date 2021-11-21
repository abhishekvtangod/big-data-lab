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

public class mapper extends Mapper<Object, Text, Text, DoubleWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
        // StringTokenizer itr = new StringTokenizer(value.toString());
        // while (itr.hasMoreTokens()) {
        // word.set(itr.nextToken());
        // context.write(word, one);
        String[] line = value.toString().split(",", 12);
        if(line.length != 12){
            System.out.println("- " + line.length);
            return;
        } 
        String outputkey = line[11];
        double outputValue = Double.parseDouble(line[8]);
        context.write(new Text(outputkey), new DoubleWritable(outputValue));

    }
}

