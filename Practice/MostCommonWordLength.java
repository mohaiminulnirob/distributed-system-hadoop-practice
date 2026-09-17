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


public class MostCommonWordLength{

    // =====================================================
    // MAPPER
    // =====================================================

    public static class MostCommonWordLengthMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable length = new IntWritable();

        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            StringTokenizer tokenizer =
                    new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {

                String currentWord =tokenizer.nextToken();
                int len= currentWord.length();
                word.set(String.valueOf(len));
                context.write(word, new IntWritable(1));

                
            }
        }
    }


    // =====================================================
    // REDUCER
    // =====================================================

    public static class MostCommonWordLengthReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int maxTime = 0;

        private List<Text> longestWords =
                new ArrayList<>();


        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum=0;
            for(IntWritable val: values){
                sum+=val.get();
            }
            if(sum>maxTime){
                maxTime=sum;
                longestWords.clear();
                longestWords.add(new Text(key));
            }
            else if(sum==maxTime){
                longestWords.add(new Text(key));
            }

        }


        // সব word process হওয়ার পরে
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Text word : longestWords) {

                context.write(
                        word,
                        new IntWritable(maxTime)
                );
            }
        }
    }


    // =====================================================
    // DRIVER
    // =====================================================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Most Common Word Length"
                );

        job.setJarByClass(
                MostCommonWordLength.class
        );


        // Mapper
        job.setMapperClass(
                MostCommonWordLengthMapper.class
        );


        // Reducer
        job.setReducerClass(
                MostCommonWordLengthReducer.class
        );


        // Mapper output
        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                IntWritable.class
        );


        // Final output
        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                IntWritable.class
        );


        // Input
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );


        // Output
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );


        // Global maximum বের করার জন্য
        // একটি reducer ব্যবহার করছি
        job.setNumReduceTasks(1);


        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}