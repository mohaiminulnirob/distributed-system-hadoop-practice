import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCount {

    // =========================
    // MAPPER
    // =========================
    public static class LetterCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text letter = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            for (int i = 0; i < line.length(); i++) {

                char ch = line.charAt(i);

                // Count only alphabetic characters
                if (Character.isLetter(ch)) {

                    // Convert to lowercase
                    ch = Character.toLowerCase(ch);

                    letter.set(String.valueOf(ch));

                    context.write(letter, one);
                }
            }
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class LetterCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
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
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Letter Count");

        job.setJarByClass(LetterCount.class);

        job.setMapperClass(LetterCountMapper.class);
        job.setReducerClass(LetterCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}