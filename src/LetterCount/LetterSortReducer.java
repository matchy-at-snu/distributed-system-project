package LetterCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterSortReducer
        extends Reducer<IndexPair, NullWritable, IndexPair, NullWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(IndexPair key, Iterable<NullWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}