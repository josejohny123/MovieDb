package trg.hadoop.mapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
// import java.lang.Math;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.Mapper.Context;

public class MovieRatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
	
	private Map<Integer, String> movieMap = new HashMap<Integer, String>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path p : files) {
			if (p.getName().equals("u.item")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));

				String line = reader.readLine();
				String[] tokens; //  = line.split("\\|");
				
				int movieID = 0, Genre = 0;
				String movieTitle = "";
				
				while(line != null) {
					tokens = line.split("\\|");
					Genre = Integer.parseInt(tokens[6]);
					
					if (Genre == 1) {
						movieID = Integer.parseInt(tokens[0]);
						movieTitle = tokens[1];
						movieMap.put(movieID, movieTitle);
					}	
					line = reader.readLine();
				}
				reader.close();
			}		
		}	
		
		if (movieMap.isEmpty()) {
			throw new IOException("Unable to load Movie data.");
		}

	}	

	@Override
	protected void reduce(IntWritable movieID, Iterable<IntWritable> ratings , Context context)
			throws IOException, InterruptedException {
		int count_rating5 = 0, count_rating4 = 0, count_rating3 = 0;
		int count_rating2 = 0, count_rating1 = 0, total_count = 0;
		Float avg_rating;
		String movieTitle = "";
		// int rating =0;
		// String[] ratingTokens;
		
		// if (! movieID.toString().isEmpty()) {
		
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
		
		if (avg_rating >= 4) { 
			movieTitle = movieMap.get(movieID.get());
			context.write(movieID, new Text(movieTitle + "," + avg_rating));
		}	
	}
	//}	
}
