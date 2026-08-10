// =============================================================
// AverageWordLengthByLetter.java
// Groups words by their first letter and computes the AVERAGE
// word length per letter. This is the classic "next step after
// WordCount" exercise because a simple IntWritable isn't enough:
// each Mapper output needs to carry BOTH a running sum and a
// running count, so we define our own Writable type.
// =============================================================

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageWordLengthByLetter {

    // -----------------------------------------------------------
    // CUSTOM WRITABLE
    // Any type that travels between Mapper and Reducer (or gets
    // written to disk during shuffle/sort) must implement Writable,
    // which just means: "I know how to serialize/deserialize myself."
    //
    // Java refresher: "implements Writable" is a CONTRACT -- it forces
    // this class to provide write() and readFields() bodies, similar
    // to how "extends Mapper" forces you to fill in map().
    // -----------------------------------------------------------
    public static class SumCount implements Writable {
        private long sum = 0;   // total length of all words seen so far
        private long count = 0; // how many words contributed to that sum

        public SumCount() { } // Hadoop requires a no-arg constructor to instantiate this via reflection

        public SumCount(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() { return sum; }
        public long getCount() { return count; }

        // How to WRITE this object's fields out as bytes, in a fixed order.
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(sum);
            out.writeLong(count);
        }

        // How to READ the bytes back -- MUST read fields in the SAME order as write().
        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readLong();
            count = in.readLong();
        }
    }

    // -----------------------------------------------------------
    // MAPPER: emits (firstLetter -> SumCount(wordLength, 1)) for every word
    // -----------------------------------------------------------
    public static class LetterMapper
            extends Mapper<Object, Text, Text, SumCount> {

        private final Text letterKey = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            for (String rawWord : line.split("\\s+")) {
                String w = rawWord.replaceAll("[^a-zA-Z]", ""); // strip punctuation
                if (w.isEmpty()) continue;

                String firstLetter = w.substring(0, 1).toLowerCase();
                letterKey.set(firstLetter);
                context.write(letterKey, new SumCount(w.length(), 1));
            }
        }
    }

    // -----------------------------------------------------------
    // REDUCER: sums up every SumCount for a letter, then divides
    // total length by total count to get the average.
    // -----------------------------------------------------------
    public static class AverageReducer
            extends Reducer<Text, SumCount, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context)
                throws IOException, InterruptedException {

            long totalSum = 0;
            long totalCount = 0;

            for (SumCount sc : values) {
                totalSum += sc.getSum();
                totalCount += sc.getCount();
            }

            double average = totalCount == 0 ? 0.0 : (double) totalSum / totalCount;
            context.write(key, new Text(String.format("avgLength=%.2f (n=%d)", average, totalCount)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageWordLengthByLetter <input path in HDFS> <output path in HDFS>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average word length by letter");

        job.setJarByClass(AverageWordLengthByLetter.class);
        job.setMapperClass(LetterMapper.class);
        job.setReducerClass(AverageReducer.class);

        // NOTE: no combiner here! Averages do NOT combine correctly the
        // naive way (average-of-averages != true average), which is a
        // classic exam trick question. Summing sub-averages would bias
        // the result toward partitions with fewer words. Summing
        // SumCount objects, as we do here, is safe because sum/count
        // are both additive.

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
