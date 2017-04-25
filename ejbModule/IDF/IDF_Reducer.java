package IDF;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDF_Reducer extends Reducer<Text, Text, Text, Text> {

	private int total_authors;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		total_authors = Integer.parseInt(conf.get("COUNT"));
		List<String> vals = new ArrayList<>();
		while (values.iterator().hasNext()) {
			String temp = values.iterator().next().toString();
			vals.add(temp);
		}
		if (key.toString().contains("COUNT")) {
			total_authors = Integer.parseInt(vals.get(0));
			System.out.println("Count of author:" + total_authors);
		} else {
			context.write(key, new Text("" + Math.log((double) total_authors / Double.parseDouble(vals.get(0)))));
		}
	}

}
