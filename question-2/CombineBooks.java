package org.hwone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import org.json.*;
import java.util.StringTokenizer

/*
*  Modify this file to combine books from the same other into
*  single JSON object. 
*  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
*  Beaware that, this may work on anynumber of nodes! 
*
*/

public class CombineBooks {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String author;
			String book;
			String line = value.toString();
			String[] authorBookTuple = line.split("\\n");

			try{
				for(int i = 0; i < authorBookTuple.length; i++){

					JSONObject obj = new JSONObject(authorBookTuple[i]);
					author = obj.getString("author");
					book = obj.getString("book");
					context.write(new Text(author), new Text(book));

				}
			}
			catch(JSONException e){
				e.printStackTrace();
			}
		}
	}

	public static class Combine extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			String books = null;
			for(Text val : values){
				if(books != null){
					books = booklist + "," + val.toString();
				}
				else{
					books = val.toString();
				}
			}
			context.write(key, new Text(books));
		}
  }

	public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			try{

				
				JSONArray array = new JSONArray();
				String[] book = null;

				for(Text val : values){
					book = val.toString().split(",");
					/*
					JSONObject tempObj = new JSONObject().put("book", val.toString());
					array.put(tempObj);
					JSONObject obj = new JSONObject();
					*/
				}
				for(int i = 0; i < book.length; i++){
					JSONObject jsonObj = new JSONObject().put("book", book[i]);
					array.put(jsonObj);
				}
				JSONObject obj = new JSONObject();
				obj.put("books", array);
				obj.put("author", key.toString());
				context.write(NullWritable.get(), new Text(obj.toString()));

			}
			catch(JSONException e){
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: CombineBooks <in> <out>");
		  System.exit(2);
	  }

	  Job job = new Job(conf, "CombineBooks");
	  job.setJarByClass(CombineBooks.class);
	  job.setMapperClass(Map.class);
	  job.setReducerClass(Reduce.class);
	  job.setCombinerClass(Combine.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  job.setOutputKeyClass(NullWritable.class);
	  job.setOutputValueClass(Text.class);
	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);

	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));

	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
