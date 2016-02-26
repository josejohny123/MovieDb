package trg.hadoop.PartitionerTrials.KeyFieldParitioner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		
		// String user_id;
		String movie_id;
		String rating;
		String KeyFields;
		
		// Long  recTime;
		// Float avgRating;
		
		// user_id = tokens[0];
		movie_id = tokens[1];
		rating = tokens[2];
		//KeyFields =movie_id+","+rating;
		KeyFields =rating+","+movie_id;
		// recTime = Long.valueOf(tokens[3]);

			
		context.write(new Text(KeyFields), value);
		}
}
