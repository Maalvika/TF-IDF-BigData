package CalcMain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TF_IDF_Mapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] data = value.toString().split("\\s+");
		if(data.length == 2) {
			context.write(new Text(data[0]), new Text("idf"+"/"+data[1]));
		} else if (data.length == 3) {
			context.write(new Text(data[1]), new Text(data[0]+"/"+data[2]));
		}
	}

	
}