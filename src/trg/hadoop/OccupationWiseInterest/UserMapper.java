package trg.hadoop.OccupationWiseInterest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// userID|Age|Gender|Occupation|PinCode
		int userID = 0;
		String Occupation = "UnKnown";
		boolean valid = true;
		String[] tokens = value.toString().split("\\|");
		
		userID = Integer.parseInt(tokens[0]);
		Occupation = tokens[3];
		
		
		if (userID <= 0 || Occupation.isEmpty()) 
			valid = false;
		
		if (valid) {
			userID = Integer.parseInt(tokens[0]);
			Occupation = tokens[3];
			context.write(new IntWritable(userID), new Text("U" + "|" + Occupation));
		}	
	}
}
