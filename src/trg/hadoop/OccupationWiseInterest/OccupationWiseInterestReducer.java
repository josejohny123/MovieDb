package trg.hadoop.OccupationWiseInterest;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OccupationWiseInterestReducer extends Reducer<IntWritable, Text, Text, Text> {
	
		public void reduce(IntWritable userID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			
			Text occupation = null,movieDetails = null;
			// Float AvgRating = 0f;  --> TBD
			
			for (Text record : values) {
				
				String ratingArray[] = record.toString().split("|");
				
				if (ratingArray[0].equals("R")) {
					movieDetails=record;
				} else if (ratingArray[0].equals("U")) {
					occupation=record;
				}
				context.write(occupation, movieDetails);
			}
		
		
	}
}

