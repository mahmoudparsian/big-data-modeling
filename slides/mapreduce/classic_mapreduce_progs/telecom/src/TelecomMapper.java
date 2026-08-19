import java.util.Date;
//
import java.io.IOException;
//
import java.text.SimpleDateFormat;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mparsian
 */
public class TelecomMapper extends
        Mapper<Object, Text, Text, LongWritable> {

    static final SimpleDateFormat DATE_FORMAT = 
    	new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //       0        |      1      |      2      |     3     |   4
    // FromPhoneNumber|ToPhoneNumber|CallStartTime|CallEndTime|STDFlag
    static final int INDEX_FROM_PHONE_NUMBER = 0;
    static final int INDEX_TO_PHONE_NUMBER = 1;
    static final int INDEX_CALL_START_TIME = 2;
    static final int INDEX_CALL_END_TIME = 3;
    static final int INDEX_STD_FLAG = 4;    
    
    @Override
    public void map(
            Object key, 
            Text value,
            Mapper<Object, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        //
        String[] parts = value.toString().split("[|]");
        //
        if (parts[INDEX_STD_FLAG].equalsIgnoreCase("1")) {
            // create key as phoneNumber
            Text phoneNumber = new Text(parts[INDEX_FROM_PHONE_NUMBER]);
            //
            String callEndTime = parts[INDEX_CALL_END_TIME];
            String callStartTime = parts[INDEX_CALL_START_TIME];
            //
            long durationAsMilliseconds = toMilliSeconds(callEndTime) - toMilliSeconds(callStartTime);
            long durationAsMinutes = durationAsMilliseconds / (1000 * 60);
            //
            // create value 
            LongWritable durationInMinutes = new LongWritable(durationAsMinutes);
            //
            // emit(phoneNumber, durationInMinutes)
            context.write(phoneNumber, durationInMinutes);
        }
    }

    /**
     * Convert date as a String into milliseconds
     * @param dateAsString
     * @return date as milliseconds
     */
    static private long toMilliSeconds(String dateAsString) {
        try {
            Date date = DATE_FORMAT.parse(dateAsString);
            return date.getTime();

        } catch (Exception e) {
            e.printStackTrace();
            return 0L;
        }
    }
}
