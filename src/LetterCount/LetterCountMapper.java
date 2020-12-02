package LetterCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] letters = value.toString().replaceAll("[^\\p{L}]", "").toLowerCase().split("");
        for (String letter : letters) {
            if (!letter.isEmpty()) {
                word.set(letter);
                context.write(word, one);
            }
        }
    }
}