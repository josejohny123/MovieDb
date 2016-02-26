package trg.hadoop.OccupationWiseInterest;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
// import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
// import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Mapper.Context;

public class RatingMapJoinMovie extends Mapper<LongWritable, Text, IntWritable, Text> {

//  movie record:	
//	movie_id:int,title:chararray,release_date:chararray, video_release_date:chararray,
//	imdb_url:chararray,unknown:int,action:int, adventure:int,animation:int,children:int,
//	comedy:int,crime:int, documentary:int,drama:int,fantasy:int,film_noir:int,horror:int, 
//	musical:int,mystery:int,romance:int,scifi:int,thriller:int, war:int,western:int
	
	
	private Map<Integer,Text> movieMap = new HashMap<Integer, Text>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path p : files) {
			if (p.getName().equals("u.item")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));

				String line = reader.readLine();
				// String[] tokens = line.split("\\|");
				
				int movieID = 0;
				Text movieItem =null;
					while(line != null) {
					String[] tokens = line.split("\\|");					
					movieID=Integer.parseInt(tokens[0]);
					movieMap.put(new Integer(movieID), new Text(line));
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
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// rating: userID	MovieID	Rating	TimeStamp
		
		Integer userID=0,movieID = 0, rating = 0;
//		String movieTitle = "";
		String movieItemString;
		Text movieItem=null;
		String record[] = value.toString().split("\t");
		
		boolean valid = true;
		
		if (record[1].isEmpty() || record[2].isEmpty())
			valid = false;

		if (valid) {
			userID= Integer.parseInt(record[0]);
			movieID = Integer.parseInt(record[1]);
			rating = Integer.parseInt(record[2]);
			
			if (movieMap.containsKey(movieID)) {
				movieItem = movieMap.get(movieID);
				movieItemString = "R"+"|"+movieItem.toString();
				context.write(new IntWritable(userID), new Text(movieItemString));
			}
		}
	}
}


