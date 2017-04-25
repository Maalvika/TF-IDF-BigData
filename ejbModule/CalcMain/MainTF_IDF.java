package CalcMain;

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

import IDF.AuthorCount_Mapper;
import IDF.AuthorCount_Reducer;
import IDF.IDF_Mapper;
import IDF.IDF_Reducer;
import IDF.WordCount_Mapper;
import IDF.WordCount_Reducer;
import TFValues.Mapper_TF;
import TFValues.Reducer_TF;

public class MainTF_IDF extends Configured implements Tool{
	
	private Configuration conf;
	private Job tfvalue, authorCount, word_per_author, idfvalues, tf_idf, aav;
	private static final String TF_INTERMEDIATE = "tfvalues";
	private static final String AUTHOR_COUNT = "authorcount";
	private static final String WORD_COUNT = "words";
	private static final String IDF_INTERMEDIATE = "idfvalues";
	private static final String TF_IDF = "final";
	private static final String AAV = "aav";

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {

			System.out.printf("Usage:<jarfile>	<inputdir> <outputdir>\n");

			System.exit(-1);

		}

		ToolRunner.run(new Configuration(), new MainTF_IDF(), args);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		calcTF();
		TextInputFormat.addInputPath(tfvalue, new Path(arg0[0]));
		TextOutputFormat.setOutputPath(tfvalue, new Path(arg0[1]+"/"+TF_INTERMEDIATE));
		tfvalue.waitForCompletion(true);
		calcAuthCount();
		TextInputFormat.addInputPath(authorCount, new Path(arg0[0]));
		TextOutputFormat.setOutputPath(authorCount, new Path(arg0[1]+"/"+AUTHOR_COUNT));
		authorCount.waitForCompletion(true);
		calcWordCount();
		TextInputFormat.addInputPath(word_per_author, new Path(arg0[0]));
		TextOutputFormat.setOutputPath(word_per_author, new Path(arg0[1]+"/"+WORD_COUNT));
		word_per_author.waitForCompletion(true);
		calcIDF(arg0);
		TextInputFormat.addInputPath(idfvalues, new Path(arg0[1]+"/"+WORD_COUNT));
		TextOutputFormat.setOutputPath(idfvalues, new Path(arg0[1]+"/"+IDF_INTERMEDIATE));
		calcTF_IDF();
		idfvalues.waitForCompletion(true);
		TextInputFormat.addInputPath(tf_idf, new Path(arg0[1]+"/"+TF_INTERMEDIATE));
		TextInputFormat.addInputPath(tf_idf, new Path(arg0[1]+"/"+IDF_INTERMEDIATE));
		TextOutputFormat.setOutputPath(tf_idf, new Path(arg0[1]+"/"+TF_IDF));
		tf_idf.waitForCompletion(true);
		aav();
		TextInputFormat.addInputPath(aav, new Path(arg0[1]+"/"+TF_IDF));
		TextOutputFormat.setOutputPath(aav, new Path(arg0[1]+"/"+AAV));
		return aav.waitForCompletion(true) ? 0:1;
		
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
	
	public void calcAuthCount() throws IOException{
		conf = new Configuration();
		authorCount = Job.getInstance(conf);
		authorCount.setJarByClass(MainTF_IDF.class);
		authorCount.setMapperClass(AuthorCount_Mapper.class);
		authorCount.setReducerClass(AuthorCount_Reducer.class);
		authorCount.setOutputKeyClass(Text.class);
		authorCount.setOutputValueClass(Text.class);
		authorCount.setInputFormatClass(TextInputFormat.class);
		authorCount.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void calcWordCount() throws IOException{
		conf = new Configuration();
		word_per_author = Job.getInstance(conf);
		word_per_author.setJarByClass(MainTF_IDF.class);
		word_per_author.setMapperClass(WordCount_Mapper.class);
		word_per_author.setReducerClass(WordCount_Reducer.class);
		word_per_author.setOutputKeyClass(Text.class);
		word_per_author.setOutputValueClass(Text.class);
		word_per_author.setInputFormatClass(TextInputFormat.class);
		word_per_author.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public void calcIDF(String args[]) throws IOException{
		conf = new Configuration();
		// set the author count before hand
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(args[1]+"/"+AUTHOR_COUNT+"/part-r-00000");
		FSDataInputStream in = fileSystem.open(path);
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, boas, 4096, false);
		String s = boas.toString();
		s = s.replace("COUNT", "").replace("[]","").trim();
		System.out.println("string author:"+s);
		String [] auths = s.split(",");
		conf.set("COUNT", ""+auths.length);
		idfvalues = Job.getInstance(conf);
		idfvalues.setJarByClass(MainTF_IDF.class);
		idfvalues.setMapperClass(IDF_Mapper.class);
		idfvalues.setReducerClass(IDF_Reducer.class);
		idfvalues.setNumReduceTasks(1);
		idfvalues.setOutputKeyClass(Text.class);
		idfvalues.setOutputValueClass(Text.class);
		idfvalues.setInputFormatClass(TextInputFormat.class);
		idfvalues.setOutputFormatClass(TextOutputFormat.class);
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
	
	public void aav() throws IOException{
		conf = new Configuration();
		aav = Job.getInstance(conf);
		aav.setJarByClass(MainTF_IDF.class);
		aav.setMapperClass(AAV_Mapper.class);
		aav.setReducerClass(AAV_Reducer.class);
		aav.setOutputKeyClass(Text.class);
		aav.setOutputValueClass(Text.class);
		aav.setInputFormatClass(TextInputFormat.class);
		aav.setOutputFormatClass(TextOutputFormat.class);
	}
	
	

}