package IDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount_Reducer extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Set<String> authorCount = new HashSet<>();
		for (Text value : values) {
			if (!authorCount.contains(value.toString())) {
				authorCount.add(value.toString());

			} 

		}
		context.write(key, new IntWritable(authorCount.size()));
		

	}
}