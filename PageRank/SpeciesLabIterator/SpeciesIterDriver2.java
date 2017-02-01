// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
 import org.apache.hadoop.io.IntWritable; 
 import org.apache.hadoop.io.Text; 
 import org.apache.hadoop.mapred.JobClient; 
 import org.apache.hadoop.mapred.JobConf; 
 import org.apache.hadoop.mapred.Mapper; 
 import org.apache.hadoop.mapred.Reducer; 

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
  
  
 public class SpeciesIterDriver2 {

	public static void main(String[] args) {
		JobClient client = new JobClient();

		int iterations = new Integer(50);
		Path inPath = new Path(args[0]);
		Path outPath = null;
		for (int i = 0; i < iterations; ++i) {

			JobConf conf = new JobConf(SpeciesIterDriver2.class);
			conf.setJobName("Species Iter");

			conf.setNumReduceTasks(5);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			if (args.length < 2) {
				System.out.println("Usage: PageRankIter <input path> <output path>");
				System.exit(0);
			}

			if (i == iterations - 1) {
				outPath = new Path(args[1]);
			} else {
				outPath = new Path(args[1] + i);
			}
			FileInputFormat.setInputPaths(conf, inPath);
			FileOutputFormat.setOutputPath(conf, outPath);

			conf.setMapperClass(SpeciesIterMapper2.class);
			conf.setReducerClass(SpeciesIterReducer2.class);
			conf.setCombinerClass(SpeciesIterReducer2.class);

			client.setConf(conf);
			try {
				FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
				if (dfs.exists(outPath)) {
					dfs.delete(outPath, true);
				}
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			inPath = outPath;
		}
	}
}