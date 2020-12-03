
package DSProject.WordCount;

import DSProject.ReverseComparator;
import DSProject.SortMapper;
import DSProject.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Application {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        FileSystem hdfs = FileSystem.get(URI.create("hdfs://<namenode-hostname>:<port>"), conf);
        // -Dproperty=value <- set things like this
        String chunkSize = System.getProperty("chunkSize");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job jobCount = Job.getInstance(conf, "letter count");

        jobCount.setJarByClass(Application.class);
        jobCount.setMapperClass(WordCountMapper.class);
        jobCount.setCombinerClass(WordCountReducer.class);
        jobCount.setReducerClass(WordCountReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(jobCount, new Path(otherArgs[i]));
        }
        if (chunkSize != null) {
            FileInputFormat.setMaxInputSplitSize(jobCount, Long.parseLong(chunkSize));
            FileInputFormat.setMinInputSplitSize(jobCount, Long.parseLong(chunkSize));
        }

        Path intermediatePath = new Path("tmp");
        FileOutputFormat.setOutputPath(jobCount, intermediatePath);

        if (!jobCount.waitForCompletion(true)) {
            System.exit(1);
        }

        Job jobSort = Job.getInstance(conf, "letter sort");
        jobSort.setJarByClass(Application.class);
        jobSort.setMapperClass(SortMapper.class);
        jobSort.setReducerClass(SortReducer.class);
        jobSort.setSortComparatorClass(ReverseComparator.class);
        jobSort.setMapOutputKeyClass(IntWritable.class);
        jobSort.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobSort, intermediatePath);
        FileOutputFormat.setOutputPath(jobSort, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(jobSort.waitForCompletion(true) ? 0 : 1);
    }
}
