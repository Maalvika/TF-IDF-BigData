package onlineTask;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import CalcMain.AAV_Reducer;
import CalcMain.MainTF_IDF;
import CalcMain.TF_IDF_Mapper;
import CalcMain.TF_IDF_Reducer;
import TFValues.Mapper_TF;
import TFValues.Reducer_TF;

public class OnlineTask_Main extends Configured implements Tool{
	
	private Configuration conf;
	Job tfvalue, tf_idf, aav, cosine, missingWord, topTen;
	private static final String TF_UNKNOWN_INTERMEDIATE = "unknowntfvalues";
	private static final String IDF_INTERMEDIATE = "idfvalues";
	private static final String TF_IDF = "unknownTF_IDF";
	private static final String KNOWN_AAV = "aav";
	private static final String UNKNOWN_AAV  = "unknown_aav";
	private static final String INTERMEDIATE  = "unknown_intermediate";
	private static final String AUTHOR_COUNT = "authorcount";
	private static final String COSINE_SIM = "cosine_sim";
	private static final String TOP_AUTH = "top_authors";
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {

			System.out.printf("Usage:<jarfile>	<inputdir> <outputdir>\n");

			System.exit(-1);

		}

		ToolRunner.run(new Configuration(), new OnlineTask_Main(), args);
		
	}

	

	@Override
	public int run(String[] arg0) throws Exception {
		calcTF();
		TextInputFormat.addInputPath(tfvalue, new Path(arg0[0]));
		TextOutputFormat.setOutputPath(tfvalue, new Path(arg0[1]+"/"+TF_UNKNOWN_INTERMEDIATE));
		tfvalue.waitForCompletion(true);
		calcTF_IDF();
		TextInputFormat.addInputPath(tf_idf, new Path(arg0[1]+"/"+TF_UNKNOWN_INTERMEDIATE));
		TextInputFormat.addInputPath(tf_idf, new Path(arg0[1]+"/"+IDF_INTERMEDIATE));
		TextOutputFormat.setOutputPath(tf_idf, new Path(arg0[1]+"/"+TF_IDF));
		tf_idf.waitForCompletion(true);
		aav();
		TextInputFormat.addInputPath(aav, new Path(arg0[1]+"/"+TF_IDF));
		TextOutputFormat.setOutputPath(aav, new Path(arg0[1]+"/"+UNKNOWN_AAV));
		aav.waitForCompletion(true);
		missingWord(arg0);
		TextInputFormat.addInputPath(missingWord, new Path(arg0[1]+"/"+UNKNOWN_AAV));
		TextInputFormat.addInputPath(missingWord, new Path(arg0[1]+"/"+KNOWN_AAV));
		TextInputFormat.addInputPath(missingWord, new Path(arg0[1]+"/"+IDF_INTERMEDIATE));
		TextOutputFormat.setOutputPath(missingWord, new Path(arg0[1]+"/"+INTERMEDIATE));
		missingWord.waitForCompletion(true);
		cosine();
		TextInputFormat.addInputPath(cosine, new Path(arg0[1]+"/"+INTERMEDIATE));
		TextOutputFormat.setOutputPath(cosine, new Path(arg0[1]+"/"+COSINE_SIM));
		cosine.waitForCompletion(true);
		topTen();
		TextInputFormat.addInputPath(topTen, new Path(arg0[1]+"/"+COSINE_SIM));
		TextOutputFormat.setOutputPath(topTen, new Path(arg0[1]+"/"+TOP_AUTH));
		return topTen.waitForCompletion(true) ? 0:1;
		
	}
	
	public void calcTF_IDF() throws IOException{
		conf = new Configuration();
		tf_idf = Job.getInstance(conf);
		tf_idf.setJarByClass(MainTF_IDF.class);
		tf_idf.setMapperClass(TF_IDF_Mapper.class);
		tf_idf.setReducerClass(TF_IDF_Reducer.class);
		tf_idf.setOutputKeyClass(Text.class);
		tf_idf.setOutputValueClass(Text.class);
		tf_idf.setInputFormatClass(TextInputFormat.class);
		tf_idf.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void calcTF() throws IOException{
		conf = new Configuration();
		tfvalue = Job.getInstance(conf);
		tfvalue.setJarByClass(MainTF_IDF.class);
		tfvalue.setMapperClass(Mapper_TF.class);
		tfvalue.setReducerClass(Reducer_TF.class);
		tfvalue.setOutputKeyClass(Text.class);
		tfvalue.setOutputValueClass(Text.class);
		tfvalue.setInputFormatClass(TextInputFormat.class);
		tfvalue.setOutputFormatClass(TextOutputFormat.class);
		
	}
	
	public void aav() throws IOException{
		conf = new Configuration();
		aav = Job.getInstance(conf);
		aav.setJarByClass(MainTF_IDF.class);
		aav.setMapperClass(UnknownAAV_Mapper.class);
		aav.setReducerClass(AAV_Reducer.class);
		aav.setOutputKeyClass(Text.class);
		aav.setOutputValueClass(Text.class);
		aav.setInputFormatClass(TextInputFormat.class);
		aav.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void missingWord(String[] args) throws IOException {
		conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(args[1]+"/"+AUTHOR_COUNT+"/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, boas, 4096, false);
		String s = boas.toString();
		s = s.replace("COUNT", "").replace("[","").replace("]", "").trim();
		System.out.println("string author:"+s);
		conf.set("authors", s);
		missingWord = Job.getInstance(conf);
		missingWord.setJarByClass(MainTF_IDF.class);
		missingWord.setMapperClass(MissingWord_Mapper.class);
		missingWord.setReducerClass(MissingWord_Reducer.class);
		missingWord.setOutputKeyClass(Text.class);
		missingWord.setOutputValueClass(Text.class);
		missingWord.setInputFormatClass(TextInputFormat.class);
		missingWord.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void cosine() throws IOException {
		conf = new Configuration();
		cosine = Job.getInstance(conf);
		cosine.setJarByClass(MainTF_IDF.class);
		cosine.setMapperClass(Cosine_Mapper.class);
		cosine.setCombinerClass(CosineCombiner.class);
		cosine.setReducerClass(Cosine_Reducer.class);
		cosine.setOutputKeyClass(Text.class);
		cosine.setOutputValueClass(Text.class);
		cosine.setInputFormatClass(TextInputFormat.class);
		cosine.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void topTen() throws IOException {
		conf = new Configuration();
		topTen = Job.getInstance(conf);
		topTen.setJarByClass(MainTF_IDF.class);
		topTen.setMapperClass(TopAuthor_Mapper.class);
		//topTen.setCombinerClass(TopAuthor_Reducer.class);
		topTen.setReducerClass(TopAuthor_Reducer.class);
		topTen.setNumReduceTasks(1);
		topTen.setOutputKeyClass(Text.class);
		topTen.setOutputValueClass(Text.class);
		topTen.setInputFormatClass(TextInputFormat.class);
		topTen.setOutputFormatClass(TextOutputFormat.class);
	}
	
}
