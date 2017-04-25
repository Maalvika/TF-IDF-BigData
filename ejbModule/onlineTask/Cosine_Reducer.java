package onlineTask;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Cosine_Reducer extends Reducer<Text, Text, DoubleWritable, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double dot_product = 0.0d,known_magnitude = 0.0d, unknown_magnitude = 0.0d;
		
		for (Text value : values) {
			String[] temp = value.toString().split("\t");
			
			dot_product +=  Double.parseDouble(temp[0]);
			known_magnitude+= Double.parseDouble(temp[1]);
			unknown_magnitude+=Double.parseDouble(temp[2]);
		}
		
		Double cosine = dot_product/(Math.sqrt(known_magnitude)* Math.sqrt(unknown_magnitude));
		
		context.write(new DoubleWritable(cosine), key);

	}
}
