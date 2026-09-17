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

public class SentenceCount {

    // =========================
    // MAPPER
    // =========================
    public static class SentenceCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final Text KEY = new Text("Total Sentences");
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (!line.isEmpty()) {

                // Split whenever we encounter ., !, or ?
                String[] sentences = line.split("[.!?]+");

                for (String sentence : sentences) {

                    // Ignore empty parts
                    if (!sentence.trim().isEmpty()) {
                        context.write(KEY, ONE);
                    }
                }
            }
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class SentenceCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                            Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }

            result.set(count);

            context.write(key, result);
        }
    }


    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sentence Count");

        job.setJarByClass(SentenceCount.class);

        job.setMapperClass(SentenceCountMapper.class);
        job.setReducerClass(SentenceCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}