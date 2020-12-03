package LetterCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LetterCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        FileSystem hdfs = FileSystem.get(URI.create("hdfs://<namenode-hostname>:<port>"), conf);
        // -Dproperty=value <- set things like this
        //System.setProperty("dfs.blocksize", "24");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

//        conf.setLong(
//                FileInputFormat.SPLIT_MAXSIZE,
//                2333
//        );

        // Job 1
        Job jobCount = Job.getInstance(conf, "letter count");

        jobCount.setJarByClass(LetterCount.class);
        jobCount.setMapperClass(LetterCountMapper.class);
        jobCount.setCombinerClass(LetterCountReducer.class);
        jobCount.setReducerClass(LetterCountReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
//        jobCount.setGroupingComparatorClass(LetterCountReducerComparator.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(jobCount, new Path(otherArgs[i]));
        }
        Path intermediatePath = new Path("tmp");
        FileOutputFormat.setOutputPath(jobCount, intermediatePath);

        if (!jobCount.waitForCompletion(true)) {
            System.exit(1);
        }

        Job jobSort = Job.getInstance(conf, "letter sort");
        jobSort.setJarByClass(LetterCount.class);
        jobSort.setMapperClass(LetterSortMapper.class);
        jobSort.setReducerClass(LetterSortReducer.class);
        jobSort.setOutputKeyClass(IntWritable.class);
        jobSort.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobSort, intermediatePath);
        FileOutputFormat.setOutputPath(jobSort, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(jobSort.waitForCompletion(true) ? 0 : 1);
    }
}
