package trg.hadoop.PartitionerTrials.KeyFieldParitioner;

import java.io.IOException;
// import java.lang.Math;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text rating, Iterable<Text> record , Context context)
			throws IOException, InterruptedException {
		for(Text reCord: record)
					context.write(rating, reCord);
	}
}
