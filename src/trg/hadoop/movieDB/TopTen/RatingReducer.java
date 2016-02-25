package trg.hadoop.movieDB.TopTen;

import java.io.IOException;
// import java.lang.Math;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
	private Map<Text, IntWritable> MovieRating = new TreeMap<Text, IntWritable>();
	private TreeMap<IntWritable, Text> MovieRatingOrder = new TreeMap<IntWritable, Text>();
	
//	protected void setup(Context context){
//		
//		MovieRatingOrder = 
//	}
	
	@Override
	protected void reduce(NullWritable test, Iterable<Text> MovieData , Context context)
			throws IOException, InterruptedException {
		int count_rating5 = 0, count_rating4 = 0, count_rating3 = 0;
		int count_rating2 = 0, count_rating1 = 0, total_count = 0,total_rating=0;
		//Float avg_rating; 
		
		for (Text moviedata : MovieData) {
			String[] tokens = moviedata.toString().split("\t");
			// String user_id;
			String movie_id;
			Integer rating;
			// Long  recTime;
			// Float avgRating;
			
			// user_id = tokens[0];
			movie_id = tokens[1];
			rating = Integer.valueOf(tokens[2]);
			// recTime = Long.valueOf(tokens[3]);
			
			if(MovieRating.containsKey(new Text(movie_id))){
				
			MovieRating.put(new Text(movie_id), new IntWritable(MovieRating.get(new Text(movie_id)).get()+rating));
				    		
			}else{
				MovieRating.put(new Text(movie_id),new IntWritable(rating));
			}
				
		
		//avg_rating = (5f * count_rating5) + (4f * count_rating4) + (3f * count_rating3); 
		//avg_rating += (2f * count_rating2) + (1f * count_rating1);				

		//total_count = count_rating5 + count_rating4 + count_rating3 
		//avg_rating = avg_rating / total_count;
	
		//MovieRatingOrder.put(new IntWritable(total_rating), movieID);
		
		//context.write(movieID,new IntWritable(total_rating));
//		if(MovieRatingOrder.size()>10){
//			MovieRatingOrder.remove(MovieRatingOrder.firstKey());
//		}
//		for(Map.Entry<IntWritable, Text> MovieMap:MovieRatingOrder.entrySet() ){
//			context.write(MovieMap.getValue(),MovieMap.getKey());
//	}
		
	

//		protected void cleanup(Context context) throws IOException, InterruptedException{
//			
//					for(Map.Entry<Text, IntWritable> MovieMap:MovieRating.entrySet() ){
//				context.write(MovieMap.getKey(),MovieMap.getValue());
//			
//			}
//		}
	

}
		for(Map.Entry<Text, IntWritable> MovieMap:MovieRating.entrySet() ){
					
			MovieRatingOrder.put(MovieMap.getValue(), MovieMap.getKey());
			if(MovieRatingOrder.size()>10){
				MovieRatingOrder.remove(MovieRatingOrder.firstKey());
			}
			}
		
		for(Map.Entry<IntWritable, Text> MovieMap:MovieRatingOrder.entrySet() ){
			context.write(MovieMap.getValue(),MovieMap.getKey());

			}
		
	}
	
}

	
	

