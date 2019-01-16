package bigdata.util;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import bigdata.Main;

public class FileFormatConverter {

	public static Path textToSequencial(String inputPath, ArrayList<Integer> columnIndexes)
			throws ClassNotFoundException, IOException, InterruptedException {
		final String outputPath = "sequence_input";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Main");

		// pass column indexes to mappers using context (encode to gson)
		Gson gson = new Gson();
		String indexes = gson.toJson(columnIndexes);

		job.getConfiguration().set("columnIndexes", indexes);
		job.setNumReduceTasks(0);
		job.setJarByClass(Main.class);
		job.setMapperClass(JobMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		Path output = new Path(outputPath);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);

		return output;
	}

	public static class JobMapper extends Mapper<Object, Text, NullWritable, Text> {

		private static ArrayList<Integer> columnIndexes = new ArrayList<Integer>();

		public void setup(Context context) {
			Gson gson = new Gson();
			String strIndexes = context.getConfiguration().get("columnIndexes");
			for (int i : gson.fromJson(strIndexes, int[].class))
				columnIndexes.add(i);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (LineParser.verifLine(value.toString(), columnIndexes))
				context.write(NullWritable.get(), value);
		}
	}
}
