import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SimpleMapReduceMRTest {
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mrDriver;

    @Before
    public void setUp() {
        mrDriver = new MapReduceDriver<>();
        mrDriver.setMapper(new SimpleMapReduce.Map());
        mrDriver.setCombiner(new SimpleMapReduce.Reduce());
        mrDriver.setReducer(new SimpleMapReduce.Reduce());
    }

    @Test
    public void filterOutByLocalMaximum1() throws IOException {
        mrDriver.withInput(new LongWritable(1), new Text("One Three Two One"));
        mrDriver.withOutput(new Text("One"), new IntWritable(2));
        mrDriver.withOutput(new Text("Three"), new IntWritable(1));
        mrDriver.withOutput(new Text("Two"), new IntWritable(1));
        mrDriver.runTest();
    }
}
