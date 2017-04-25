package TFValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_TF extends Reducer<Text, Text, Text, Text> {

	private static int ONE = 1;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//System.out.println("Author:"+key.toString());
		Map<String, Integer> wordCounts = new HashMap<>();
		for (Text value : values) {
			String[] words = value.toString().replaceAll("[^-a-zA-Z0-9 ]", " ").replaceAll("--+", " ").trim().split("\\s+");
			for (String word : words) {
				if (!word.isEmpty() && word != null && !word.contains("null")) {
					if (wordCounts.containsKey(word)) {
						//System.out.println("writing: "+word);
						int t = wordCounts.get(word).intValue();
						t++;
						wordCounts.put(word, t);
						
					} else {
						//System.out.println("writing: "+word);
						wordCounts.put(word, ONE);
					}
				}
			}

		}

		int maxValue = findMaxValue(wordCounts);
		for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
			String k = entry.getKey();
			Integer v = entry.getValue();
			Double tf = 0.5 + 0.5 * ((double)v/(double)maxValue);
			context.write(key, new Text(k + " " + tf));
		}

	}
	
	public int findMaxValue(Map<String, Integer> wordMap) {
		int max = 0;
		for (Map.Entry<String, Integer> entry : wordMap.entrySet()){
			if (max < entry.getValue()){
				max = entry.getValue();
			}
		}
		return max;
	}

}