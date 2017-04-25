package onlineTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MissingWord_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String[] total_authors =conf.get("authors").split(",");
		Map<String,Double> tempAuths = new HashMap<>();
		double idf_word = 0;
		Double unknown_if_idf = null;
		for(Text value: values) {
			String[] temp = value.toString().split("/");
			if(temp[0].equalsIgnoreCase("idf")){
				idf_word = Double.parseDouble(temp[1]);
			} else if(temp[0].equalsIgnoreCase("UNKNOWN")){
				unknown_if_idf = Double.parseDouble(temp[1]);
			}
			else {
				tempAuths.put(temp[0].trim(), Double.parseDouble(temp[1].trim()));
			}
		}
		
		for(String auth:total_authors){
			if(!tempAuths.containsKey(auth)) {
				tempAuths.put(auth, (double)0.5*idf_word);
			}
		}
		
		if(unknown_if_idf!=null) {
			for(Entry<String, Double> entry: tempAuths.entrySet()) {
				double knownValue =  entry.getValue();
				double knownSq = Math.pow(knownValue, 2);
				double unknownSq = Math.pow(unknown_if_idf, 2);
				double nr = knownValue * unknown_if_idf;
				context.write(new Text(entry.getKey().trim()), new Text(nr+"\t"+knownSq+"\t"+unknownSq));
			}
		}
	}

}

