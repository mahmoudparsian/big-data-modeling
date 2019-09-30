import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.commons.lang.StringUtils;

/**
 * A reducer class that just emits the sum of the input values.
 *
 * @author Mahmoud Parsian
 *
 */
public class TopMovieReducer extends Reducer<Text, Text, Text, Text> {
	
	// This method is called once for each key. Most applications will 
	// define their reduce class by overriding this method. The default 
	// implementation is an identity function.
	// key = userID
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
        int maxRating = 0;
        String movieID = null;
        for (Text val : values) {
           String[] tokens = StringUtils.split(val.toString(), ",");
       	   // tokens[0] = movieID
           // tokens[1] = rating
           int rating = Integer.parseInt(tokens[1]);
           if (rating > maxRating) {
           		maxRating = rating;
           		movieID = tokens[0];
           }
        }
        context.write(key, new Text(movieID));
   	}
}

