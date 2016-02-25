package trg.hadoop.reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		
		private int  tot_male_ratings = 0, tot_female_ratings = 0;
		
		public void reduce(IntWritable userID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String Gender = ""; 
			int rating_count = 0;
			// Float AvgRating = 0f;  --> TBD
			
			for (Text record : values) {
				
				String ratingArray[] = record.toString().split(",");
				
				if (ratingArray[0].equals("R")) {
					rating_count ++;
				} else if (ratingArray[0].equals("U")) {
					Gender = ratingArray[1];
				}
			}
			
			if (Gender.equals("M"))
				tot_male_ratings += rating_count;
			else
				tot_female_ratings += rating_count;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("M"), new IntWritable(tot_male_ratings));
			context.write(new Text("F"), new IntWritable(tot_female_ratings));
		}
	
}
