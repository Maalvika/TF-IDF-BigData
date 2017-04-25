package CalcMain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AAV_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		Map<String, String> aav = new HashMap<>();
		for(Text value: values) {
			String temp = value.toString();
			String [] val = temp.split("\\s+");
			aav.put(val[0], val[1]);
		}
		    
		context.write(key, new Text("_START_"+aav+"_END_"));
		
	}

}

