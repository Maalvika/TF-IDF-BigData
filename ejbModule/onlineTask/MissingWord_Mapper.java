package onlineTask;


import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MissingWord_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	//private Map<String, String> unknownWords;

	/*public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(conf.get("output_dir") + "/" + conf.get("unknownAAV") + "/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, boas, 4096, false);
		String aav = boas.toString();
		String[] vector = aav.split("_START_");
		unknownWords = convertAAVToMap(vector);

	}
*/
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if(value.toString().contains("_START_")) {
			String[] vector = value.toString().split("_START_");
			String author = vector[0].trim();
			Map<String, String> knownAuthor = convertAAVToMap(vector);
			
			for(Entry<String, String> entry: knownAuthor.entrySet()) {
				context.write(new Text(entry.getKey().trim()), new Text(author+"/"+entry.getValue().trim()));
			}
		} else {
			String[] idf = value.toString().split("\\s+");
			if(idf.length == 2){
				context.write(new Text(idf[0].trim()), new Text("idf/"+idf[1].trim()));
			}
		}
		
	}

	private Map<String, String> convertAAVToMap(String[] vector) {

		Map<String, String> tempMap = new TreeMap<>();
		String[] words = vector[1].replace("_END_", "").replace("{", "").replace("}", "").split(",");
		for (int i = 0; i < words.length; i++) {
			String[] temp = words[i].trim().split("=");
			tempMap.put(temp[0].trim(), temp[1].trim());
		}
		return tempMap;
	}

}
