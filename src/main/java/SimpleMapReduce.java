import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

public class SimpleMapReduce extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(SimpleMapReduce.class);
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static  IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final static Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord  = new Text();
            for ( String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord.set(word);
                context.write(currentWord,one);
            }
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance( getConf(), "Simple MapReduce App");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SimpleMapReduce.Map.class);
        job.setReducerClass(SimpleMapReduce.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SimpleMapReduce(), args);
        System.exit(res);
    }

}
