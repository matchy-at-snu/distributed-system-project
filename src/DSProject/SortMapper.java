package DSProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper
        extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String letter = tokens[0].trim();
        int count = Integer.parseInt(tokens[1].trim());
        context.write(new IntWritable(count), new Text(letter));
    }
}
