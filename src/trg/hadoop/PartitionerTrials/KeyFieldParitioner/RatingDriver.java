package trg.hadoop.PartitionerTrials.KeyFieldParitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingDriver extends Configured implements Tool{

@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		job.setJobName(this.getClass().getName());
		
		job.setJarByClass(getClass());

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);		
		
		
		// configure output and input source
		TextInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		

		job.setMapperClass(RatingMapper.class);
		// job.setCombinerClass(RatingReducer.class);
		//job.setPartitionerClass(RatingPartitioner.class);
		
		job.setPartitionerClass(KeyFieldBasedPartitioner.class);
		
		conf.set("mapreduce.partition.keypartitioner.options", "-K 1,1");
		conf.set("mapreduce.map.output.key.field.seperator", ",");
		conf.setInt("num.key.fields.for.partition", 1);
		//mapreduce.partition.keypartitioner.options,"-k 1,1"
		//mapreduce.map.output.key.field.seperator,","
		//num.key.field.for.partition,"1"
		
		job.setReducerClass(RatingReducer.class);
		job.setNumReduceTasks(5);
		
		// configure output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
}
public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RatingDriver(), args);
		System.exit(exitCode);
	}
}
