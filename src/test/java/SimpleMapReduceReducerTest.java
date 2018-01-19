import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleMapReduceReducerTest {
    private ReduceDriver<Text, IntWritable, Text, IntWritable> rDriver;

    @Before
    public void setUp() {
        rDriver = new ReduceDriver<>();
        rDriver.setReducer(new SimpleMapReduce.Reduce());
    }

    @Test
    public void reduceTest() throws IOException {
        IntWritable one = new IntWritable(1);
        IntWritable two = new IntWritable(2);
        List<IntWritable> values2 = new ArrayList<>();
        values2.add(one);
        values2.add(one);
        List<IntWritable> values1 = new ArrayList<>();
        values1.add(one);

        rDriver.withInput(new Text("One"), values2);
        rDriver.withInput(new Text("Two"), values1);
        rDriver.withInput(new Text("Three"), values1);

        rDriver.withOutput(new Text("One"), two);
        rDriver.withOutput(new Text("Two"), one);
        rDriver.withOutput(new Text("Three"), one);
        rDriver.runTest();
    }
}
