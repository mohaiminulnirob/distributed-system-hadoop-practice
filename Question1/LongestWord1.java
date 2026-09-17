import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWord1 {

    // ============================================================
    // MAPPER
    // ============================================================
    public static class LongestWordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable length = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Read one line
            String line = value.toString();

            // Split line into words
            String[] words = line.split("\\s+");

            for (String w : words) {

                // Remove punctuation
                w = w.replaceAll("[\\p{Punct}]", "");

                // Ignore empty words
                if (w.isEmpty()) {
                    continue;
                }

                // Set word as key
                word.set(w);

                // Set word length as value
                length.set(w.length());

                // Emit (word, length)
                context.write(word, length);
            }
        }
    }

    // ============================================================
    // REDUCER
    // ============================================================
    public static class LongestWordReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int maxLength = 0;

        private List<String> longestWords = new ArrayList<>();

        @Override
        public void reduce(Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            // Since key is a word, its length is the same
            // for all values associated with that key.
            int length = values.iterator().next().get();

            // If this word is longer than the current maximum
            if (length > maxLength) {

                maxLength = length;

                longestWords.clear();

                longestWords.add(key.toString());
            }

            // If this word has the same maximum length
            else if (length == maxLength) {

                longestWords.add(key.toString());
            }
        }

        // Called after all keys have been processed
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // Output all longest words
            for (String w : longestWords) {

                context.write(
                        new Text(w),
                        new IntWritable(maxLength));
            }
        }
    }

    // ============================================================
    // DRIVER
    // ============================================================
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Find Longest Words");

        job.setJarByClass(LongestWord1.class);

        // Set Mapper
        job.setMapperClass(LongestWordMapper.class);

        // Set Reducer
        job.setReducerClass(LongestWordReducer.class);

        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input path
        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        // Output path
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        // IMPORTANT:
        // All words must go to the same reducer
        // so that we can find the global maximum.
        job.setNumReduceTasks(1);

        // Run the job
        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}