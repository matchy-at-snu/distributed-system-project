package LetterCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterSortMapper
        extends Mapper<Object, Text, IndexPair, NullWritable> {

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        String letter = tokens[0].trim();
        int count = Integer.parseInt(tokens[1].trim());
        context.write(new IndexPair(letter, count), NullWritable.get());
    }
}
