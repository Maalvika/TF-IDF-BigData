package IDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorCount_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Set<String> authors = new HashSet<String>();
		for (Text value : values) {
			authors.add(value.toString());

		}
		System.out.println("called:"+authors.size());
		context.write(new Text("COUNT"), new Text(authors.toString()));
	}
}