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

public class WhitespaceCounter {

    // =========================
    // MAPPER
    // =========================
    public static class WhitespaceMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final Text KEY = new Text("Whitespace Count");
        private IntWritable result = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            int count = 0;

            // Check every character
            for (int i = 0; i < line.length(); i++) {

                if (Character.isWhitespace(line.charAt(i))) {
                    count++;
                }
            }

            result.set(count);

            context.write(KEY, result);
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class WhitespaceReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                            Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }


    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Whitespace Counter");

        job.setJarByClass(WhitespaceCounter.class);

        job.setMapperClass(WhitespaceMapper.class);
        job.setReducerClass(WhitespaceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}