package LetterCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterSortReducer
        extends Reducer<IntWritable, Text, Text, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}