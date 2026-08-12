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


public class VowelConsonantCount {

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

            while (tokenizer.hasMoreTokens()) {
                String str=tokenizer.nextToken();
                if(str.charAt(0)=='a'||str.charAt(0)=='e'||str.charAt(0)=='i'||str.charAt(0)=='o'||str.charAt(0)=='u'){
                    word.set("vowel");
                }
                else{
                    word.set("consonant");
                }
                context.write(word, one);

            
            }
        }
    }


    // REDUCER
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }


    // DRIVER
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "case insensitive word count"
        );

        job.setJarByClass(
                VowelConsonantCount.class
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

// apple → Vowel
// banana → Consonant
// orange → Vowel

// output
// Vowel → 2
// Consonant → 1