package bigdata;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import com.google.gson.Gson;
import bigdata.util.LineParser;

public class KMeans {

	private static List<Integer> columns;
	private static int k;
	private static Path finalOutputPath;
	
	public static void init(List<Integer> _columns, int _k,Path path) {
		columns = _columns;
		k = _k;
		finalOutputPath = path;
	}
	
	public static void startIteration()
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Main");
		job.getConfiguration().set("columns", (new Gson()).toJson(columns));
		job.getConfiguration().setInt("numberDimentions", columns.size());
		

		// delete the old new_centroids.json file (if exists), and create a new empty file.
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path outputPath = new Path("new_centroids.json");
		fs.createNewFile(outputPath);

		job.setNumReduceTasks(k);

		job.setJarByClass(Main.class);
		job.setMapperClass(JobMapper.class);

		job.setReducerClass(JobReducer.class);
		job.setCombinerClass(JobCombiner.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PointAcc.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PointAcc.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path("sequence_input"));
		Path output = new Path("iteration_output");
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	public static class JobMapper extends Mapper<NullWritable, Text, IntWritable, PointAcc> {

		private static List<Centroid> centroids;
		private static List<Integer> columns;

		public void setup(Context context) throws IOException {
			centroids = CentroidPersistance.load(new Path("current_centroids.json"));

			// get the columns from the context
			columns = new ArrayList<Integer>();
			int[] temp = (new Gson()).fromJson(context.getConfiguration().get("columns"), int[].class);
			for (int i : temp)
				columns.add(i);
		}

		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			PointAcc point = LineParser.parsePoint(value.toString(), columns);
			Centroid closest = centroids.get(0);
			double minDistance;
			try {
				minDistance = point.distanceTo(closest);
				for (Centroid c : centroids) {
					double dist = point.distanceTo(c);
					if (dist < minDistance) {
						minDistance = dist;
						closest = c;
					}
				}
				context.write(new IntWritable(closest.getId()), point);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class JobCombiner extends Reducer<IntWritable, PointAcc, IntWritable, PointAcc> {

		private static int nbDimentions;

		public void setup(Context context) {
			nbDimentions = context.getConfiguration().getInt("numberDimentions", 0);
		}

		public void reduce(IntWritable key, Iterable<PointAcc> values, Context context)
				throws IOException, InterruptedException {
			PointAcc moy = new PointAcc(nbDimentions);
			for (PointAcc p : values)
				moy.accumulate(p);
			context.write(key, moy);
		}
	}

	public static class JobReducer extends Reducer<IntWritable, PointAcc, IntWritable, PointAcc> {

		private static int nbDimentions;

		public void setup(Context context) {
			nbDimentions = context.getConfiguration().getInt("numberDimentions", 0);
		}

		public void reduce(IntWritable key, Iterable<PointAcc> values, Context context)
				throws IOException, InterruptedException {
			
			PointAcc moy = new PointAcc(nbDimentions);
			for (PointAcc pm : values) 
				moy.accumulate(pm);

			Path newCentroidsPath = new Path("new_centroids.json");
			int id = CentroidPersistance.nbCentroids(newCentroidsPath) + 1;
			CentroidPersistance.append(new Centroid(moy.getCentroid(), id), newCentroidsPath);
		}
	}

	public static class FinalMapper extends Mapper<NullWritable, Text, NullWritable, Text> {

		private static List<Centroid> centroids;
		private static List<Integer> columns;

		public void setup(Context context) throws IOException {
			centroids = CentroidPersistance.load(new Path("current_centroids.json"));

			columns = new ArrayList<Integer>();
			int[] temp = (new Gson()).fromJson(context.getConfiguration().get("columns"), int[].class);

			for (int i : temp)
				columns.add(i);
		}

		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			PointAcc point = LineParser.parsePoint(value.toString(), columns);
			Centroid closest = centroids.get(0);
			double minDistance;
			try {
				minDistance = point.distanceTo(closest);
				for (Centroid c : centroids) {
					double dist = point.distanceTo(c);
					if (dist < minDistance) {
						minDistance = dist;
						closest = c;
					}
				}
				String out = value.toString() + "," + closest.getId();
				context.write(NullWritable.get(), new Text(out));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void finalIteration()
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Main");
		job.getConfiguration().set("columns", (new Gson()).toJson(columns));
		job.getConfiguration().setInt("numberDimentions", columns.size());

		job.setNumReduceTasks(0);

		job.setJarByClass(Main.class);
		job.setMapperClass(FinalMapper.class);

		// job.setReducerClass(JobReducer.class);
		// job.setCombinerClass(JobCombiner.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path("sequence_input"));

		FileOutputFormat.setOutputPath(job, finalOutputPath);
		job.waitForCompletion(true);
	}

}
