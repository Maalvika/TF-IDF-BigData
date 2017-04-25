package IDF;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCount_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String transformedString = value.toString().toLowerCase();
		String[] subportions = transformedString.split("<===>");
		String author = getAuthorFromSentence(subportions[0]);
		if(subportions.length == 3 && !subportions[2].isEmpty()) {
			String[] words = subportions[2].toString().replaceAll("[^-a-zA-Z0-9 ]", " ").replaceAll("--+", " ").trim().split("\\s+");
			for (String word : words) {	
				if(!word.isEmpty() && word!= "") {
					context.write(new Text(word), new Text(author));
				}
			}
		}

	}

	private String getAuthorFromSentence(String name) {
		int commaI = name.lastIndexOf(" ");
		if(commaI == -1) {
			return name.substring(0).trim();
		}
		return name.substring(commaI).trim();
	}

}