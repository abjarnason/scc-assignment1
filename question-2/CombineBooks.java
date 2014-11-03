package my.books;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.*;

import javax.security.auth.callback.TextInputCallback;

public class CombineBooks {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String author;
            String book;
            String line = value.toString();
            String[] tuple = line.split("\\n");
            try{
                for(int i=0;i<tuple.length; i++){
                    JSONObject obj = new JSONObject(tuple[i]);
                    author = obj.getString("author");
                    book = obj.getString("book");
                    context.write(new Text(author), new Text(book));
                }
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            try{
                JSONObject obj = new JSONObject();
                JSONArray ja = new JSONArray();
                for(Text val : values){
                    JSONObject jo = new JSONObject().put("book", val.toString());
                    ja.put(jo);
                }
                obj.put("books", ja);
                obj.put("author", key.toString());
                context.write(NullWritable.get(), new Text(obj.toString()));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: CombineBooks <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "CombineBooks");
        job.setJarByClass(CombineBooks.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
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

// package org.hwone;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.util.GenericOptionsParser;

// //TODO import necessary components

// /*
// *  Modify this file to combine books from the same other into
// *  single JSON object. 
// *  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
// *  Beaware that, this may work on anynumber of nodes! 
// *
// */

// public class CombineBooks {

//   //TODO define variables and implement necessary components

//   public static void main(String[] args) throws Exception {
//     Configuration conf = new Configuration();
//     String[] otherArgs = new GenericOptionsParser(conf, args)
// 				.getRemainingArgs();
//     if (otherArgs.length != 2) {
//       System.err.println("Usage: CombineBooks <in> <out>");
//       System.exit(2);
//     }

//     //TODO implement CombineBooks

//     Job job = new Job(conf, "CombineBooks");

//     //TODO implement CombineBooks

//     System.exit(job.waitForCompletion(true) ? 0 : 1);
//   }
// }
