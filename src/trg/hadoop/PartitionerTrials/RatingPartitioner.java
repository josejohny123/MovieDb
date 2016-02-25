package trg.hadoop.PartitionerTrials;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RatingPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text KeyPart, Text valuePart, int num_reducers) {
		// TODO Auto-generated method stub
		return Integer.valueOf(KeyPart.toString().split(",")[0])%num_reducers;
	}

}


//mapreduce.partition.keypartitioner.options,"-k 1,1"
//mapreduce.map.output.key.field.seperator,","
//num.key.field.for.partition,"1"