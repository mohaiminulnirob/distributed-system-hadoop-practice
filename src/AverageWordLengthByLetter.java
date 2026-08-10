import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageWordLengthByLetter {

    // ============================================================
    // CUSTOM WRITABLE
    // Stores:
    // sum = total length of words
    // count = number of words
    // ============================================================

    public static class SumCount implements Writable {

        private long sum;
        private long count;

        public SumCount() {
        }

        public SumCount(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(sum);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readLong();
            count = in.readLong();
        }
    }

    // ============================================================
    // MAPPER
    //
    // Input:
    // line number -> line
    //
    // Output:
    // first letter -> (word length, 1)
    //
    // Example:
    // apple
    //
    // Output:
    // a -> (5, 1)
    // ============================================================

    public static class AverageMapper
            extends Mapper<LongWritable, Text, Text, SumCount> {

        private Text letter = new Text();

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split("\\s+");

            for (String word : words) {

                if (word.length() == 0) {
                    continue;
                }

                // Convert to lowercase
                word = word.toLowerCase();

                // First letter
                String firstLetter = word.substring(0, 1);

                letter.set(firstLetter);

                // Emit:
                // first letter -> word length and count
                context.write(
                        letter,
                        new SumCount(word.length(), 1));
            }
        }
    }

    // ============================================================
    // REDUCER
    //
    // Input:
    // a -> [(5,1), (3,1), (4,1)]
    //
    // Calculate:
    //
    // totalSum = 5 + 3 + 4 = 12
    // totalCount = 1 + 1 + 1 = 3
    //
    // average = 12 / 3 = 4.0
    //
    // Output:
    // a -> 4.0
    // ============================================================

    public static class AverageReducer
            extends Reducer<Text, SumCount, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(
                Text key,
                Iterable<SumCount> values,
                Context context)
                throws IOException, InterruptedException {

            long totalSum = 0;
            long totalCount = 0;

            for (SumCount value : values) {

                totalSum += value.getSum();
                totalCount += value.getCount();
            }

            double average = (double) totalSum / totalCount;

            result.set(average);

            context.write(key, result);
        }
    }

    // ============================================================
    // DRIVER
    // ============================================================

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: AverageWordLengthByLetter <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Average Word Length By Letter");

        job.setJarByClass(AverageWordLengthByLetter.class);

        // Mapper
        job.setMapperClass(AverageMapper.class);

        // Reducer
        job.setReducerClass(AverageReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));

        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}