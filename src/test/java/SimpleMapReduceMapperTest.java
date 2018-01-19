import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SimpleMapReduceMapperTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mDriver;

    @Before
    public void setUp() {
        mDriver = new MapDriver<>();
        mDriver.setMapper(new SimpleMapReduce.Map());
    }

    @Test
    public void filterOutByLocalMaximum() throws IOException {
        mDriver.withInput(new LongWritable(1), new Text("One Three Two One"));
        mDriver.withOutput(new Text("One"), new IntWritable(1));
        mDriver.withOutput(new Text("Three"), new IntWritable(1));
        mDriver.withOutput(new Text("Two"), new IntWritable(1));
        mDriver.withOutput(new Text("One"), new IntWritable(1));
        mDriver.runTest();
    }
}