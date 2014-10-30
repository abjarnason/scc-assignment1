package org.hwone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

//TODO import necessary components

/*
*  Modify this file to return single combined books from the author which
*  is queried as QueryAuthor <in> <out> <author>. 
*  i.e. QueryAuthor in.txt out.txt Tobias Wells 
*  {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
*  Beaware that, this may work on anynumber of nodes! 
*
*/

public class QueryAuthor {

  //TODO define variables and implement necessary components

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
    if (otherArgs.length < 3) {
      System.err.println("Usage: QueryAuthor <in> <out> <author>");
      System.exit(2);
    }

    //TODO implement QueryAuthor

    Job job = new Job(conf, "QueryAuthor");

    //TODO implement QueryAuthor

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
