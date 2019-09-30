import java.io.IOException;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mparsian
 */
public class TelecomReducer extends
        Reducer<Text, LongWritable, Text, LongWritable> {


    @Override
    public void reduce(
            Text key, 
            Iterable<LongWritable> values,
            Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        //
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        //
        //
        if (sum >= 60) {
            LongWritable result = new LongWritable(sum);
            context.write(key, result);
        }
        else {
            // no output is emitted
        }

    }
}
