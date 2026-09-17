import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNWords {

 
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                word.set(w.toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class TopNReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String> topWords = new TreeMap<>();

        private int N = 10;

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            String word = key.toString();

            topWords.put(sum, word);

            if (topWords.size() > N) {
                topWords.remove(topWords.firstKey());
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<Integer, String> entry : topWords.descendingMap().entrySet()) {

                context.write(
                        new Text(entry.getValue()),
                        new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Top N Words");

        job.setJarByClass(TopNWords.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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