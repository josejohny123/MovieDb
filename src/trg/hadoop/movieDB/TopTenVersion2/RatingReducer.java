package trg.hadoop.movieDB.TopTenVersion2;

import java.io.IOException;
// import java.lang.Math;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import trg.hadoop.movieDB.TopTenVersion2.RatingMapper.Rating;

public class RatingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	 private  TreeMap<Integer,Text> MovieRatings =new TreeMap<Integer, Text>();
	 private  TreeMap<Text,Integer> MovieRatingsorder = new TreeMap<Text, Integer>();
		
	@Override
	protected void reduce(Text movieID, Iterable<IntWritable> ratings , Context context)
			throws IOException, InterruptedException {
		int count_rating5 = 0, count_rating4 = 0, count_rating3 = 0;
		int count_rating2 = 0, count_rating1 = 0;
				
				int total_count = 0;
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
		
		
		MovieRatings.put(total_count,new Text(movieID));
		MovieRatingsorder.put(new Text(movieID),total_count);
		if(MovieRatings.size()>10){
			MovieRatings.remove(MovieRatings.firstKey());
		}
//		context.getCounter(Rating.no_reduce).increment(1);
//		
//		System.out.println("From Reducer"+context.getCounter(Rating.no_reduce).getValue());;
//		context.getCounter(Rating.treemapCount).increment(1);
//		
	}
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.getCounter(Rating.no_cleanup).increment(1);
			System.out.println("From CleanUp"+context.getCounter(Rating.no_cleanup).getValue());;
			
			for(Entry<Integer, Text>  moviemap: MovieRatings.descendingMap().entrySet()){
				
			context.write(moviemap.getValue(), new IntWritable(moviemap.getKey()));
			}
			
						
		}
	

}
