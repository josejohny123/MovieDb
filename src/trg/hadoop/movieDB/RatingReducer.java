package trg.hadoop.movieDB;

import java.io.IOException;
// import java.lang.Math;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	protected void reduce(Text movieID, Iterable<IntWritable> ratings , Context context)
			throws IOException, InterruptedException {
		int count_rating5 = 0, count_rating4 = 0, count_rating3 = 0;
		int count_rating2 = 0, count_rating1 = 0, total_count = 0;
		Float avg_rating; 
		
		for (IntWritable rating : ratings) {
			switch (rating.get()) {
			case 5:
				count_rating5++;				
				break;
			case 4:
				count_rating4++;
				break;
			case 3:
				count_rating3++;
				break;
			case 2:
				count_rating2++;
				break;
			case 1:
				count_rating1++;
				break;
			default:
				break;
			}
		}
		avg_rating = (5f * count_rating5) + (4f * count_rating4) + (3f * count_rating3); 
		avg_rating += (2f * count_rating2) + (1f * count_rating1);				

		total_count = count_rating5 + count_rating4 + count_rating3 + count_rating2 + count_rating1;
		
		avg_rating = avg_rating / total_count;
		
		context.write(movieID, new Text(avg_rating + "," + total_count));
	}
}
