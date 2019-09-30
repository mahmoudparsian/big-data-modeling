import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

/**
 * LogHandler Mapper
 *
 * For each line of input, break the line into words
 * and emit them as (<b>word</b>, <b>1</b>).
 *
 * @author Mahmoud Parsian
 *
 */

public class LogHandlerMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text reducerKey = new Text();

	// called once at the beginning of the task.	
	//protected void setup(Context context)
    //	throws IOException,InterruptedException {
	//	this.ignoredLength = context.getConfiguration().getInt("word.count.ignored.length", 3);
	//}

	// called once for each key/value pair in the input split. 
	// most applications should override this, but the default 
	// is the identity function.
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		
	  	String line = value.toString().toLowerCase();	  	
	  	if (line.contains("error"))  {
			reducerKey.set("error");
			context.write(reducerKey, one);
		}
	  	
	  	if (line.contains("exception"))  {
			reducerKey.set("exception");
			context.write(reducerKey, one);
		}

	  	if (line.contains("warning"))  {
			reducerKey.set("warning");
			context.write(reducerKey, one);
		}	
	}
	
}
