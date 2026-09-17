import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalary {

    // =========================
    // MAPPER
    // =========================
    public static class SalaryMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private static final Text KEY = new Text("Average Salary");

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (!line.isEmpty()) {

                // Example:
                // John,25000

                String[] parts = line.split(",");

                if (parts.length == 2) {

                    try {
                        double salary = Double.parseDouble(parts[1].trim());

                        context.write(
                                KEY,
                                new DoubleWritable(salary));

                    } catch (NumberFormatException e) {
                        // Ignore invalid salary
                    }
                }
            }
        }
    }

    // =========================
    // REDUCER
    // =========================
    public static class SalaryReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;

                result.set(average);

                context.write(key, result);
            }
        }
    }

    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Average Salary");

        job.setJarByClass(AverageSalary.class);

        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(SalaryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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

// John,25000
// Rahim,30000
// Karim,35000
// Sakib,40000