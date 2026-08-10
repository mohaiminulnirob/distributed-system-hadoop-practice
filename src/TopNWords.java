// =============================================================
// TopNWords.java
// Finds the N most frequent words in the input. This builds on
// WordCount but demonstrates the "single reducer collects global
// top-N" pattern, which shows up constantly in distributed-systems
// exams (top-K queries, most-viewed pages, etc.).
//
// Strategy:
//   Job 1 (reuses WordCount's Mapper/Reducer) -> word, count pairs
//   Job 2 here reads THAT output and keeps only the top N globally
//   by funnelling everything through a SINGLE reducer that maintains
//   a small sorted structure (TreeMap) instead of writing every record.
// =============================================================

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNWords {

    // How many top words to keep. Hardcoded for simplicity; you could
    // also read this from conf.getInt(...) if you want it configurable.
    private static final int N = 10;

    // -----------------------------------------------------------
    // MAPPER
    // Input here is the OUTPUT of a prior WordCount job:
    // lines that look like "word<TAB>count"
    // -----------------------------------------------------------
    public static class ParseMapper extends Mapper<Object, Text, NullWritable, Text> {

        // Every mapper keeps its OWN local top-N in a TreeMap keyed by count,
        // so we don't ship every single word across the network -- only
        // this mapper's local top N candidates. This is basically a
        // manual "combiner" pattern for a top-N problem, where a normal
        // sum-based combiner wouldn't apply.
        private final TreeMap<Integer, String> localTop = new TreeMap<>();

        @Override
        public void map(Object key, Text value, Context context) {
            String line = value.toString();
            String[] parts = line.split("\\t");
            if (parts.length != 2) return;

            try {
                String word = parts[0];
                int count = Integer.parseInt(parts[1].trim());
                localTop.put(count, word);

                // Keep only the top N in this local map; drop the smallest once we exceed N.
                if (localTop.size() > N) {
                    localTop.remove(localTop.firstKey());
                }
            } catch (NumberFormatException e) {
                // skip malformed lines
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit this mapper's local top-N candidates once, at the end of its input split.
            for (Map.Entry<Integer, String> entry : localTop.entrySet()) {
                context.write(NullWritable.get(),
                        new Text(entry.getValue() + "\t" + entry.getKey()));
            }
        }
    }

    // -----------------------------------------------------------
    // REDUCER
    // Because every key is NullWritable, ALL records land in exactly
    // ONE reducer -- that's what makes this a correct GLOBAL top-N.
    // -----------------------------------------------------------
    public static class TopNReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

        private final TreeMap<Integer, String> globalTop = new TreeMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) {
            for (Text val : values) {
                String[] parts = val.toString().split("\\t");
                if (parts.length != 2) continue;
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);

                globalTop.put(count, word);
                if (globalTop.size() > N) {
                    globalTop.remove(globalTop.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // descendingMap() so the highest count is printed first.
            for (Map.Entry<Integer, String> entry : globalTop.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TopNWords <wordcount output path> <top-N output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top N words");

        job.setJarByClass(TopNWords.class);
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(TopNReducer.class);

        // Force exactly one reducer -- required for a true GLOBAL top-N.
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
