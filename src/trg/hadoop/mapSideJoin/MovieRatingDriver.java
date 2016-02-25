package trg.hadoop.mapSideJoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieRatingDriver extends Configured implements Tool{
	
@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		job.setJobName("Map side Join");
		
		job.setJarByClass(MovieRatingDriver.class);
		
	    try{
	        DistributedCache.addCacheFile(new URI("/user/trg03/movieName/u.item"), job.getConfiguration());
	        }catch(Exception e){
	        	System.out.println(e);
        }
		
		job.setMapperClass(MovieRatingMapper.class);
		job.setReducerClass(MovieRatingReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
/*		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
*/		
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
}

public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieRatingDriver(), args);
		
		System.exit(exitCode);
	}
}
