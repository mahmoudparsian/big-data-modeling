import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

/**
 * Top Movie Mapper
 *
 * K: hadoop generated, ignoired
 * V: userID,movieID,rating
 *
 * @author Mahmoud Parsian
 *
 */

public class TopMovieMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	private Text reducerKey = new Text();
	private Text reducerValue = new Text();

	// called once for each key/value pair in the input split. 
	// most applications should override this, but the default 
	// is the identity function.
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		// value = userID,movieID,rating
	  	String line = value.toString().trim();	  	
	  	if (line == null) {
	  		return;
	  	}
	  	
        String[] tokens = StringUtils.split(line, ",");
        if (tokens == null) {
        	return;
        }
        
        // tokens[0] = userID
        // tokens[1] = movieID
        // tokens[2] = rating
        String outputValue = tokens[1] + "," + tokens[2];
		reducerKey.set(tokens[0]);
		reducerValue.set(outputValue);
		context.write(reducerKey, reducerValue);
	}
}
