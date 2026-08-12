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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

public class ShortestWordMultiple{
    //Mapper 
    public static class  ShortestWordMapper extends Mapper<Object, Text, Text, IntWritable>{
        Text word = new Text();
        IntWritable length = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line =value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                String currWord=tokenizer.nextToken();
                word.set(currWord);
                length.set(currWord.length());
                context.write(word,length);
            }
        }
    }
    public static class ShortestWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            int minLength=Integer.MAX_VALUE;
           ArrayList<Text> shortestWords = new ArrayList<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          
           String word=key.toString();
             if(word.length()<minLength){
                 minLength=word.length();
                 shortestWords.clear();
                 shortestWords.add(new Text(key));

             }
             else if(word.length()==minLength){
                 shortestWords.add(new Text(key));
             }
           
        //    context.write(key, new IntWritable(minLength));
            
        }
        @Override
            protected void cleanup(Context context) 
            throws IOException, InterruptedException {
                for(Text word:shortestWords){
                context.write(word,new IntWritable(minLength));
                }
            }
    }
    //driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Shortest Word");
        job.setJarByClass(ShortestWordMultiple.class);
        job.setMapperClass(ShortestWordMapper.class);
        job.setReducerClass(ShortestWordReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
