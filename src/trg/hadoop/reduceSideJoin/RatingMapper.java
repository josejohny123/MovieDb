package trg.hadoop.reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// rating: userID	MovieID	Rating	TimeStamp
		
		int userID  = 0;
		int movieID = 0;
		int rating = 0;
		
		String record[] = value.toString().split("\t");
		
		boolean valid = true; 
		for (int i=1; i < 3; i++) {
			if (record[i].isEmpty())
				valid = false;
		}

		if (valid) {
			userID  = Integer.parseInt(record[0]);
			movieID = Integer.parseInt(record[1]);
			rating = Integer.parseInt(record[2]);
			
			context.write(new IntWritable(userID), new Text("R" + "," + movieID + "," + rating));
		}
	}
}
