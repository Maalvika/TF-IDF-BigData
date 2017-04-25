package onlineTask;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopAuthor_Reducer extends Reducer<Text, Text, Text, Text> {

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		TreeMap<Double, String> sourtedOutput = new TreeMap<>(Collections.reverseOrder());
		for(Text value: values){
			String [] temp = value.toString().split("\\s+");
			sourtedOutput.put(Double.parseDouble(temp[0]), temp[1]);
		}
		
		int count = 0;
		for(Entry<Double, String> entry: sourtedOutput.entrySet()) {
			if(count<10) {
				context.write(new Text(entry.getKey().toString()), new Text(entry.getValue()));
				count++;
				
			} else {
				break;
			}
			
		}

	}
	
}

