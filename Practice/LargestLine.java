import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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


public class LargestLine{

    // MAPPER
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable length = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            int len=line.length();
            length.set(len);
            word.set(line);
            context.write(word, length);
            
        }
    }


    // REDUCER
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        ArrayList<Text> largestLines = new ArrayList<>();
        private int maxLength = 0;

        public void reduce(Text key,
                   Iterable<IntWritable> values,
                   Context context)
        throws IOException, InterruptedException {

            int len = key.toString().length();

            if (len > maxLength) {

                maxLength = len;

                largestLines.clear();

                largestLines.add(new Text(key));

            }
            else if (len == maxLength) {

                largestLines.add(new Text(key));
            }
        }
        @Override
        protected void cleanup(Context context)
                        throws IOException, InterruptedException {
            for (Text line : largestLines) {
                context.write(line, new IntWritable(maxLength));
            }
        }
   }


    // DRIVER
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "largest line"
        );

        job.setJarByClass(
                LargestLine.class
        );

        job.setMapperClass(
                TokenizerMapper.class
        );

        job.setReducerClass(
                IntSumReducer.class
        );

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

