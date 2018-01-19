import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleMapReduceDriverTest {
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");
//        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("src/test/resources/testfile.txt");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        SimpleMapReduce driver = new SimpleMapReduce();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
    }
}
