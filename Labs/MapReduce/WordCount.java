1.	package org.myorg;
2.	
3.	import java.io.IOException;
4.	import java.util.*;
5.	
6.	import org.apache.hadoop.fs.Path;
7.	import org.apache.hadoop.conf.*;
8.	import org.apache.hadoop.io.*;
9.	import org.apache.hadoop.mapred.*;
10.	import org.apache.hadoop.util.*;
11.	
12.	public class WordCount {
13.	
14.	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
15.	     private final static IntWritable one = new IntWritable(1);
16.	     private Text word = new Text();
17.	
18.	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
19.	       String line = value.toString();
20.	       StringTokenizer tokenizer = new StringTokenizer(line);
21.	       while (tokenizer.hasMoreTokens()) {
22.	         word.set(tokenizer.nextToken());
23.	         output.collect(word, one);
24.	       }
25.	     }
26.	   }
27.	
28.	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
29.	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
30.	       int sum = 0;
31.	       while (values.hasNext()) {
32.	         sum += values.next().get();
33.	       }
34.	       output.collect(key, new IntWritable(sum));
35.	     }
36.	   }
37.	
38.	   public static void main(String[] args) throws Exception {
39.	     JobConf conf = new JobConf(WordCount.class);
40.	     conf.setJobName("wordcount");
41.	
42.	     conf.setOutputKeyClass(Text.class);
43.	     conf.setOutputValueClass(IntWritable.class);
44.	
45.	     conf.setMapperClass(Map.class);
46.	     conf.setCombinerClass(Reduce.class);
47.	     conf.setReducerClass(Reduce.class);
48.	
49.	     conf.setInputFormat(TextInputFormat.class);
50.	     conf.setOutputFormat(TextOutputFormat.class);
51.	
52.	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
53.	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
54.	
55.	     JobClient.runJob(conf);
57.	   }
58.	}


