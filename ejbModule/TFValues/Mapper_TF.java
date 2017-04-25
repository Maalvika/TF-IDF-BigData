package TFValues;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_TF extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String transformedString = value.toString().toLowerCase();
		String[] subportions = transformedString.split("<===>");
		String author = getAuthorFromSentence(subportions[0]);
		System.out.println("author:"+author);
		if (subportions.length == 3 && !subportions[2].isEmpty()) {

			context.write(new Text(author), new Text(subportions[2]));
		} 
	}

	private String getAuthorFromSentence(String name) {
		int commaI = name.lastIndexOf(" ");
		if (commaI == -1) {
			return name.substring(0).trim();
		}
		return name.substring(commaI).trim();
	}

}