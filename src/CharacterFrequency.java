import java.io.IOException;

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

public class CharacterFrequency {

    // =========================
    // MAPPER
    // =========================

    public static class CharacterMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text character = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Process every character in the line
            for (int i = 0; i < line.length(); i++) {

                char ch = line.charAt(i);

                // Ignore spaces
                if (ch != ' ') {

                    character.set(String.valueOf(ch));

                    context.write(character, one);
                }
            }
        }
    }


    // =========================
    // REDUCER
    // =========================

    public static class CharacterReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {

                sum += value.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }


    // =========================
    // DRIVER
    // =========================

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: CharacterFrequency <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Character Frequency"
        );

        job.setJarByClass(CharacterFrequency.class);

        // Mapper
        job.setMapperClass(CharacterMapper.class);

        // Reducer
        job.setReducerClass(CharacterReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input path
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        // Output path
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}