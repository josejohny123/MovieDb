package trg.hadoop.OccupationWiseInterest;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OccupationWiseInterestDriver extends Configured implements Tool{
	
@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		job.setJobName("Map side Join");
		
		job.setJarByClass(OccupationWiseInterestDriver.class);
		
	    try{
	        DistributedCache.addCacheFile(new URI("/user/trg03/movieName/u.item"), job.getConfiguration());
	        }catch(Exception e){
	        	System.out.println(e);
        }
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, RatingMapJoinMovie.class);
		
		job.setReducerClass(OccupationWiseInterestReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
/*		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
*/		
		
		Path outputPath = new Path(args[2]);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
}

public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new OccupationWiseInterestDriver(), args);
		
		System.exit(exitCode);
	}
}

