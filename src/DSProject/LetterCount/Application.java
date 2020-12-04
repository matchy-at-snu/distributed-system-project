package DSProject.LetterCount;

import DSProject.OutputWriter;
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

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 1) {
            // TODO: better usage message
            System.err.println("Usage: wordcount <in> [<in>...] -Doutput <out> -DchunkSize <chunkSize>");
            System.exit(2);
        }

        String chunkSize = System.getProperty("chunkSize");
//        String output = System.getProperty("output");

        Job jobCount = Job.getInstance(conf, "letter count");

        jobCount.setJarByClass(Application.class);
        jobCount.setMapperClass(LetterCountMapper.class);
        jobCount.setCombinerClass(LetterCountReducer.class);
        jobCount.setReducerClass(LetterCountReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobCount, new Path(otherArgs[0]));
        if (chunkSize != null) {
            FileInputFormat.setMaxInputSplitSize(jobCount, Long.parseLong(chunkSize));
            FileInputFormat.setMinInputSplitSize(jobCount, Long.parseLong(chunkSize));
        }

        Path intermediatePath = new Path("tmp/inter");
        Path rawOutputPath = new Path("tmp/out");
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
        FileOutputFormat.setOutputPath(jobSort, rawOutputPath);

//        System.exit(jobSort.waitForCompletion(true) ? 0 : 1);
        if (!jobSort.waitForCompletion(true)) {
            System.exit(1);
        }

        String output = otherArgs[otherArgs.length - 1];
        if (output != null) {
            System.out.println(rawOutputPath.toString());
            OutputWriter.writeToOutput(rawOutputPath.toString(), output);
        } else {
            OutputWriter.writeToOutput(rawOutputPath.toString());
        }

        System.exit(0);
    }
}
