package trg.hadoop.movieDB.TopTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	enum Rating {
		 rating_lt_3,
		 rating_ge_3
	};
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		
		// String user_id;
		String movie_id;
		Integer rating;
		// Long  recTime;
		// Float avgRating;
		
		// user_id = tokens[0];
		movie_id = tokens[1];
		rating = Integer.valueOf(tokens[2]);
		// recTime = Long.valueOf(tokens[3]);

		if (rating < 3)
			context.getCounter(Rating.rating_lt_3).increment(1);
		else 
			context.getCounter(Rating.rating_ge_3).increment(1);
		
		
		context.write(NullWritable.get(), value);
		}
}
