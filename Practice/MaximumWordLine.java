import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaximumWordLine {

    // MAPPER
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            
            StringTokenizer tokenizer =
                    new StringTokenizer(line);
                    
            int count=0;

            while (tokenizer.hasMoreTokens()) {
                count++;
                tokenizer.nextToken();
            }
            context.write(value, new IntWritable(count));
        }
    }


    // REDUCER
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

            private int maxWords = 0;
            private Text maxLine = new Text();

            public void reduce(Text key,
                            Iterable<IntWritable> values,
                            Context context)
                    throws IOException, InterruptedException {

                IntWritable firstValue = values.iterator().next();

                int count = firstValue.get();

                if (count > maxWords) {
                    maxWords = count;
                    maxLine.set(key);
                }
            }

            @Override
            protected void cleanup(Context context)
                    throws IOException, InterruptedException {

                context.write(
                        maxLine,
                        new IntWritable(maxWords)
                );
            }
    }


    // DRIVER
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "maximum word line"
        );

        job.setJarByClass(
                MaximumWordLine.class
        );

        job.setMapperClass(
                TokenizerMapper.class
        );

        job.setReducerClass(
                IntSumReducer.class
        );

        job.setNumReduceTasks(1);

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
                job.waitForCompletion(true)
                ? 0
                : 1
        );
    }
}
