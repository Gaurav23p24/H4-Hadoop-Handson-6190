package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import com.example.WordMapper;
import com.example.WordReducer;

public class Controller {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(Controller.class);

        // Mapper & Reducer
        job.setMapperClass(WordMapper.class);

        // Safe combiner: sums counts locally before shuffle
        job.setCombinerClass(IntSumReducer.class);

        // Final reducer does counting again (robust) + sorts in cleanup
        job.setReducerClass(WordReducer.class);

        // Ensure a single reducer so the sorted output is global
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}










// package com.example.controller;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

// import com.example.WordMapper;
// import com.example.WordReducer;

// public class Controller{

//     public static void main(String[] args) throws Exception{
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "word count");
//         job.setJarByClass(Controller.class);
//         job.setMapperClass(WordMapper.class);
//         job.setCombinerClass(WordReducer.class);
//         job.setReducerClass(WordReducer.class);
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);
//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));
//         System.exit(job.waitForCompletion(true) ? 0 : 1);
    
//     }
// }