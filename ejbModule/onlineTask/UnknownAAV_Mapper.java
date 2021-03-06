package onlineTask;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnknownAAV_Mapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] data = value.toString().split("\\s+");
		context.write(new Text("UNKNOWN"), new Text(data[1]+"\t"+data[2]));
	}

	
}