package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text outWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // normalize: lowercase + remove non-alphanumeric (keep spaces)
        String line = value.toString()
                           .toLowerCase()
                           .replaceAll("[^a-z0-9\\s]", " ");

        // split on 1+ whitespace
        String[] tokens = line.split("\\s+");
        for (String token : tokens) {
            if (token.length() >= 3) {      // assignment rule
                outWord.set(token);
                context.write(outWord, ONE);
            }
        }
    }
}











// package com.example;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;
// import java.io.IOException;
// import java.util.StringTokenizer;

// public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
//     private final static IntWritable one = new IntWritable(1);
//     private Text word = new Text();

//     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//         StringTokenizer itr = new StringTokenizer(value.toString());
//         while (itr.hasMoreTokens()) {
//             String currentWord = itr.nextToken();
//             // Only process words with three or more characters
//             if (currentWord.length() >= 3) {
//                 word.set(currentWord);
//                 context.write(word, one);
//             }
//         }
//     }
// }