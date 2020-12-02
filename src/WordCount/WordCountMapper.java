package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] rawTokens = value.toString().split("[^\\p{L}-]|--+");
        for (String rawToken : rawTokens) {
            String token = rawToken.toLowerCase()
                                 .replaceAll("^-+", "")
                                 .replaceAll("-+$", "");
            if (!token.isEmpty()) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}