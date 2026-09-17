import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWord {

    // =========================
    // MAPPER
    // =========================
    public static class LongestWordMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text one = new Text("1");
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Split the line into tokens
            String[] words = line.split("\\s+");

            for (String w : words) {

                // Remove punctuation
                w = w.replaceAll("[^a-zA-Z0-9]", "");

                // Ignore empty strings
                if (!w.isEmpty()) {

                    // Keep the original case
                    word.set(w);

                    // Send every word to the same reducer
                    context.write(one, word);
                }
            }
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class LongestWordReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int maxLength = 0;

            List<String> longestWords = new ArrayList<>();

            for (Text value : values) {

                String word = value.toString();

                int length = word.length();

                // Found a new maximum
                if (length > maxLength) {

                    maxLength = length;

                    longestWords.clear();

                    longestWords.add(word);
                }

                // Found another word with the same maximum length
                else if (length == maxLength) {

                    longestWords.add(word);
                }
            }

            // Output all longest words
            for (String word : longestWords) {
                context.write(new Text(word), new Text(""));
            }
        }
    }


    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Longest Word");

        job.setJarByClass(LongestWord.class);

        // Mapper
        job.setMapperClass(LongestWordMapper.class);

        // Reducer
        job.setReducerClass(LongestWordReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
