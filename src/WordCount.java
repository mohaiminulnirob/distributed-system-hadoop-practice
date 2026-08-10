// =============================================================
// WordCount.java
// The "hello world" of MapReduce. Counts how many times each
// word appears across one or more text files stored in HDFS.
// =============================================================

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

public class WordCount {

    // -----------------------------------------------------------
    // MAPPER
    // Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    //   KEYIN    = LongWritable  -> the byte offset of the line (we ignore it, hence "_")
    //   VALUEIN  = Text          -> one line of the input file
    //   KEYOUT   = Text          -> the word we emit
    //   VALUEOUT = IntWritable   -> the count for that word (always 1 here)
    //
    // Java refresher: "extends Mapper<...>" means this class IS-A Mapper,
    // and we override its map() method to define our own behaviour.
    // -----------------------------------------------------------
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        // Reused objects instead of creating new ones every call (performance habit
        // Hadoop code always follows -- avoids allocating millions of small objects).
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // StringTokenizer splits the line on whitespace by default.
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // context.write(...) is how a Mapper emits a key/value pair
                // downstream to the shuffle/sort phase.
                context.write(word, one);
            }
        }
    }

    // -----------------------------------------------------------
    // REDUCER
    // Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    //   KEYIN    = Text        -> a word (same type as Mapper's KEYOUT)
    //   VALUEIN  = IntWritable -> an Iterable of all the "1"s emitted for that word
    //   KEYOUT   = Text        -> the word again
    //   VALUEOUT = IntWritable -> the total count
    //
    // Hadoop groups ALL values sharing the same key and calls reduce() ONCE
    // per key, handing you an Iterable of every value that arrived for it.
    // This grouping is the "shuffle & sort" step happening between Map and Reduce.
    // -----------------------------------------------------------
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // -----------------------------------------------------------
    // DRIVER (main method)
    // Wires the Mapper and Reducer together into a runnable Job.
    // -----------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path in HDFS> <output path in HDFS>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // same logic can run as a local pre-reducer
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // waitForCompletion(true) blocks until the job finishes and prints progress.
        // It returns true on success -- we translate that into a proper exit code.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
