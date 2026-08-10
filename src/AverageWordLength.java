import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageWordLength {

    // =========================
    // MAPPER
    // =========================

    public static class AverageMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final Text KEY = new Text("average");
        private LongWritable wordLength = new LongWritable();

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().split("\\s+");

            for (String word : words) {

                if (word.length() > 0) {

                    wordLength.set(word.length());

                    context.write(KEY, wordLength);
                }
            }
        }
    }

    // =========================
    // REDUCER
    // =========================

    public static class AverageReducer
            extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(
                Text key,
                Iterable<LongWritable> values,
                Context context)
                throws IOException, InterruptedException {

            long totalLength = 0;
            long wordCount = 0;

            for (LongWritable value : values) {

                totalLength += value.get();
                wordCount++;
            }

            double average = (double) totalLength / wordCount;

            result.set(average);

            context.write(key, result);
        }
    }

    // =========================
    // DRIVER
    // =========================

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: AverageWordLength <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Average Word Length");

        job.setJarByClass(AverageWordLength.class);

        // Mapper
        job.setMapperClass(AverageMapper.class);

        // Reducer
        job.setReducerClass(AverageReducer.class);

        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input
        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        // Output
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}