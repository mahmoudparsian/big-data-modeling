package com.nextbio.async.scoring.mapreduce.poc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
//
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author mparsian
 */
public class TelecomDriver {

    public static void main(String[] args) throws Exception {
        //
        if (args.length != 2) {
            System.err.println("Usage: TelecomDriver <input> <output>");
            System.exit(2);
        }
        //
        String inputPath = args[0];
        String outputPath = args[1];
        //
        // create a Job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "telecom");
        job.setJarByClass(TelecomDriver.class);
        //
        // plugin mapper, combiner, reducer
        job.setMapperClass(TelecomMapper.class);
        job.setCombinerClass(TelecomReducer.class);
        job.setReducerClass(TelecomReducer.class);
        //
        // define reducers's (key, value) data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //
        // define Input/Output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //
        // submit the job to cluster and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
