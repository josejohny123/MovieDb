package trg.hadoop.mapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
// import java.util.HashMap;
import java.util.HashSet;
// import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Mapper.Context;

public class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

//  movie record:	
//	movie_id:int,title:chararray,release_date:chararray, video_release_date:chararray,
//	imdb_url:chararray,unknown:int,action:int, adventure:int,animation:int,children:int,
//	comedy:int,crime:int, documentary:int,drama:int,fantasy:int,film_noir:int,horror:int, 
//	musical:int,mystery:int,romance:int,scifi:int,thriller:int, war:int,western:int
	
	
	private Set<Integer> movieSet = new HashSet<Integer>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path p : files) {
			if (p.getName().equals("u.item")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));

				String line = reader.readLine();
				// String[] tokens = line.split("\\|");
				
				int movieID = 0, Genre = 0;
				
				while(line != null) {
					String[] tokens = line.split("\\|");
					Genre = Integer.parseInt(tokens[6]);
					
					if (Genre == 1) {
						movieID = Integer.parseInt(tokens[0]);
						movieSet.add(movieID);
					}	
					line = reader.readLine();
				}
				reader.close();
			}		
		}	
		
		if (movieSet.isEmpty()) {
			throw new IOException("Unable to load Movie data.");
		}

	}	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// rating: userID	MovieID	Rating	TimeStamp
		
		Integer movieID = 0, rating = 0;
//		String movieTitle = "";
		
		String record[] = value.toString().split("\t");
		
		boolean valid = true;
		
		if (record[1].isEmpty() || record[2].isEmpty())
			valid = false;

		if (valid) {
			movieID = Integer.parseInt(record[1]);
			rating = Integer.parseInt(record[2]);
			
			if (movieSet.contains(movieID)) {
				context.write(new IntWritable(movieID), new IntWritable(rating));
			}
		}
	}
}
