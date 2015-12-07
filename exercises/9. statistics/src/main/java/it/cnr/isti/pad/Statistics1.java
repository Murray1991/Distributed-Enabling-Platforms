package it.cnr.isti.pad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Statistics1
{
	public static class NewMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private Text native_age = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split(",\\s");
			native_age.set(tokens[tokens.length - 2]);
			context.write(native_age, new IntWritable(Integer.parseInt(tokens[0])));
		}
	}

	public static class NewReducer extends Reducer<Text, IntWritable, Text, Text>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int max_age = 0, min_age = Integer.MAX_VALUE;
			for (IntWritable val : values) {
				min_age = Math.min(min_age, val.get());
				max_age = Math.max(max_age, val.get());
			}
			context.write(key, new Text(min_age + " " + max_age));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "statistics1");
		job.setJarByClass(Statistics1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);
		
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
