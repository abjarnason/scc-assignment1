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
import org.apache.hadoop.conf.*;

import java.io.IOException;
import org.json.*;
import java.util.StringTokenizer;

/*
*  Modify this file to return single combined books from the author which
*  is queried as QueryAuthor <in> <out> <author>. 
*  i.e. QueryAuthor in.txt out.txt Tobias Wells 
*  {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
*  Beaware that, this may work on anynumber of nodes! 
*
*/

public class QueryAuthor {

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
					books = books + "," + val.toString();
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

			Configuration conf = context.getConfiguration();
			String input = conf.get("authorName");

			try{
				JSONArray array = new JSONArray();
				String[] book = null;
				for(Text val : values){
					book = val.toString().split(",");
				}
				for(int i = 0; i < book.length; i++){
					JSONObject jsonObj = new JSONObject().put("book", book[i]);
					array.put(jsonObj);
				}
				JSONObject obj = new JSONObject();
				obj.put("books", array);
				obj.put("author", key.toString());
				String author = obj.getString("author");

				if(input.equals(author)){
					context.write(NullWritable.get(), new Text(obj.toString()));
				}
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
		if (otherArgs.length < 3) {
			System.err.println("Usage: QueryAuthor <in> <out> <author>");
			System.exit(2);
		}

		String input = null;
		for(int i = 2; i < otherArgs.length; i++){
			if(input != null){
				input = input + " " + otherArgs[i];
			}
			else{
				input = otherArgs[i];
			}
		}

		conf.set("authorName", input);

		Job job = new Job(conf, "QueryAuthor");
		job.setJarByClass(QueryAuthor.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//conf.set("authorQuery", args[2]);


		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
