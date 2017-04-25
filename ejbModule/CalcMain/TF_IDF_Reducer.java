package CalcMain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TF_IDF_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		Map<String, Double> auth_tf = new HashMap<>();
		Double idf = null;
		for(Text value: values) {
			String temp = value.toString();
			String [] val = temp.split("/");
			if(value.toString().contains("idf")) {
				idf = Double.parseDouble(val[1]);
			} else {
				auth_tf.put(val[0],Double.parseDouble(val[1]));
			}
		}
		for (Map.Entry<String, Double> entry : auth_tf.entrySet()) {
			Double d = entry.getValue();
		    context.write(new Text(entry.getKey()), new Text(key+" "+d*idf));
		}
	}

}
